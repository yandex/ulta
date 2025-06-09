import copy
import logging
import typing
from datetime import timedelta
from queue import Full, Empty
from ulta.common.config import UltaConfig
from ulta.common.agent import AgentInfo
from ulta.common.collections import QueueLike, Deque
from ulta.common.reporter import NullReporter, Reporter, ReporterHandlerProtocol
from ulta.common.interfaces import ClientFactory, RemoteLoggingClient, LogMessage
from ulta.common.logging import SinkHandler
from ulta.common.utils import truncate_string
from ulta.service.service_context import LabelContext
from ulta.service.log_uploader_service import (
    MESSAGE_MAX_LENGTH as CLOUD_LOGGING_MESSAGE_MAX_LENGTH,
    CHUNK_MAX_SIZE as CLOUD_LOGGING_CHUNK_MAX_SIZE,
)


CONTEXT_LABELS_KEY = 'context_labels'


class LogMessageProcessor(ReporterHandlerProtocol):
    def __init__(
        self,
        log_group_id: str,
        agent_id: str,
        client: RemoteLoggingClient,
        error_handler: typing.Callable[[Exception, logging.Logger], None],
        max_message_length: int | None = None,
        max_labels_size: int = -1,
        min_level: int = logging.NOTSET + 1,
        max_batch_size: int | None = None,
    ):
        self._min_level = min_level
        self._agent_id = agent_id
        self._log_group_id = log_group_id
        self._client = client
        self._error_handler = error_handler
        self._max_message_length = max_message_length
        self._max_labels_size = max_labels_size
        self._max_batch_size = max_batch_size

    def get_max_batch_size(self) -> int | None:
        return self._max_batch_size

    def handle(self, request_id: str, messages: list[logging.LogRecord]):
        messages = [m for m in messages if m.levelno >= self._min_level]
        if not messages:
            return

        log_data = [self.prepare_log_record(item) for item in messages]
        self._client.send_log(
            log_group_id=self._log_group_id,
            log_data=log_data,
            resource_type='agent_logs',
            resource_id=self._agent_id,
            request_id=request_id,
        )

    def error_handler(self, error: Exception, logger: logging.Logger):
        if self._error_handler is not None:
            self._error_handler(error, logger)

    @staticmethod
    def _get_object_str(obj: object) -> str:
        if obj is None:
            return ''
        elif isinstance(obj, str):
            return obj
        return str(obj)

    @staticmethod
    def _get_args_pair_size(arg_pair: tuple[str, object]) -> int:
        if not isinstance(arg_pair[1], str):
            arg_pair = (arg_pair[0], LogMessageProcessor._get_object_str(arg_pair[1]))
        assert isinstance(arg_pair[1], str)
        return len(arg_pair[0]) + len(arg_pair[1])

    @staticmethod
    def _make_labels(source: typing.Iterable[tuple[str, object]], max_size: int) -> tuple[dict[str, str], int]:
        remaining_size = max_size
        if max_size < 0:
            remaining_size = 1_000_000

        result = {}
        for p in source:
            p1 = LogMessageProcessor._get_object_str(p[1])
            pair_size = LogMessageProcessor._get_args_pair_size((p[0], p1))
            if pair_size > remaining_size:
                if remaining_size < len(p[0]):
                    break

                result[p[0]] = truncate_string(p1, remaining_size - len(p[0]), cut_in_middle=False)
                break
            remaining_size -= pair_size
            result[p[0]] = p1
        return result, max_size if max_size < 0 else remaining_size

    def _prepare_labels(self, item: logging.LogRecord) -> dict[str, str]:
        result: dict[str, str] = {}
        remaining_size = self._max_labels_size
        context_labels = get_extra(item, CONTEXT_LABELS_KEY)
        if isinstance(context_labels, typing.Mapping):
            result, remaining_size = self._make_labels(context_labels.items(), self._max_labels_size)

        if not isinstance(item.args, typing.Mapping):
            return result

        args_labels, _ = self._make_labels(sorted(item.args.items(), key=lambda p: len(repr(p[1]))), remaining_size)
        result.update(args_labels)
        return result

    def prepare_log_record(self, item: logging.LogRecord) -> LogMessage:
        labels = self._prepare_labels(item)
        return LogMessage(
            level=item.levelno,
            created_at=item.created,
            labels=labels,
            message=truncate_string(item.getMessage(), self._max_message_length),
        )


def make_log_reporter(
    logger: logging.Logger,
    config: UltaConfig,
    agent: AgentInfo,
    transport_factory: ClientFactory,
    label_context: LabelContext | None,
    cached_logs: QueueLike | None = None,
) -> Reporter | NullReporter:
    all_reporters: list[ReporterHandlerProtocol] = list(
        filter(
            None,
            [
                _make_cloud_logging_log_reporter(config, agent, transport_factory),
                _make_loadtesting_backend_log_reporter(config, agent, transport_factory),
            ],
        )
    )
    if not all_reporters:
        return NullReporter()

    record_transformer = make_label_context_record_transformer(label_context)
    handler = init_log_sink(size=10_000, label_context=label_context)
    if cached_logs is not None:
        try:
            while True:
                r = cached_logs.get_nowait()
                if record_transformer is not None:
                    r = record_transformer(r)
                handler.sink.put_nowait(r)
        except (Full, Empty):
            pass

    logger.addHandler(handler)

    max_unsent_size = config.log_max_unsent_queue_size or 1000
    return Reporter(
        handler.sink,
        logger=logger,
        handlers=all_reporters,
        retention_period=config.log_retention_period or timedelta(hours=3),
        report_interval=5,
        max_unsent_size=max_unsent_size,
    )


def make_label_context_record_transformer(label_context: LabelContext | None):
    if label_context is None:
        return None

    def transformer(r: logging.LogRecord):
        return with_extra(r, {CONTEXT_LABELS_KEY: copy.copy(label_context.labels)})

    return transformer


# exploiting LogRecord extra params: https://github.com/python/cpython/blob/3.12/Lib/logging/__init__.py#L1657
def with_extra(r: logging.LogRecord, extra: dict) -> logging.LogRecord:
    if not extra:
        return r
    return logging.makeLogRecord(r.__dict__ | extra)


def get_extra(r: logging.LogRecord, key: str):
    return r.__dict__.get(key)


def _make_cloud_logging_log_reporter(
    config: UltaConfig,
    agent: AgentInfo,
    transport_factory: ClientFactory,
):
    if not config.log_group_id or not agent.id:
        return None

    client = transport_factory.create_logging_client()

    def error_handler(e: Exception, logger: logging.Logger):
        logger.warning('Failed to send logs to YC Log group %s: %s', config.log_group_id, e)

    return LogMessageProcessor(
        log_group_id=config.log_group_id,
        agent_id=agent.id,
        client=client,
        error_handler=error_handler,
        max_message_length=CLOUD_LOGGING_MESSAGE_MAX_LENGTH,
        max_batch_size=CLOUD_LOGGING_CHUNK_MAX_SIZE,
    )


def _make_loadtesting_backend_log_reporter(
    config: UltaConfig,
    agent: AgentInfo,
    transport_factory: ClientFactory,
):
    if not agent.id or not config.report_log_events:
        return None

    client = transport_factory.create_events_log_client(agent)

    def error_handler(e: Exception, logger: logging.Logger):
        logger.warning('Failed to send event logs to loadtesting: %s', e)

    return LogMessageProcessor(
        log_group_id='log_events',
        agent_id=agent.id,
        client=client,
        error_handler=error_handler,
        max_labels_size=8192,
        max_message_length=2000,
        max_batch_size=config.log_max_chunk_size,
    )


def init_log_sink(
    queue: QueueLike | None = None, size: int = 0, label_context: LabelContext | None = None
) -> SinkHandler:
    if queue is None:
        queue = Deque(maxlen=size)
    return SinkHandler(queue, make_label_context_record_transformer(label_context))


def release_log_sink(handler: SinkHandler):
    # purge queue
    try:
        while True:
            handler.sink.get_nowait()
    except Empty:
        pass
    handler.close()
