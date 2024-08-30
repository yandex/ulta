import logging
import typing
from datetime import timedelta
from queue import Full, Empty, Queue
from ulta.common.config import UltaConfig
from ulta.common.agent import AgentInfo
from ulta.common.reporter import NullReporter, Reporter
from ulta.common.interfaces import ClientFactory, RemoteLoggingClient
from ulta.common.logging import LogMessage, create_sink_handler
from ulta.common.utils import truncate_string


class LogReporter:
    def __init__(
        self,
        log_group_id: str,
        agent_id: str,
        client: RemoteLoggingClient,
        additional_labels: dict[str, str | None] | None = None,
        max_message_length: int | None = None,
        max_labels_size: int = -1,
    ):
        self._agent_id = agent_id
        self._log_group_id = log_group_id
        self._client = client
        additional_labels = additional_labels or {}
        self._labels = {k: str(v) for k, v in additional_labels.items() if v is not None}
        self._max_message_length = max_message_length
        self._max_labels_size = max_labels_size

    def handle(self, request_id: str, records: list[LogMessage | logging.LogRecord]):
        messages: list[LogMessage] = []
        for r in records:
            if isinstance(r, LogMessage):
                messages.append(r)
            else:
                labels = self._args_as_mapping(r.args)
                labels.update(self._labels)
                messages.append(LogMessage(r, labels))

        self._client.send_log(
            log_group_id=self._log_group_id,
            log_data=messages,
            resource_type='agent_logs',
            resource_id=self._agent_id,
            request_id=request_id,
        )

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
            arg_pair = (arg_pair[0], LogReporter._get_object_str(arg_pair[1]))
        assert isinstance(arg_pair[1], str)
        return len(arg_pair[0]) + len(arg_pair[1])

    @staticmethod
    def _make_labels(source: typing.Iterable[tuple[str, object]], max_size: int) -> tuple[dict[str, str], int]:
        remaining_size = max_size
        if max_size < 0:
            remaining_size = 1_000_000

        result = {}
        for p in source:
            p1 = LogReporter._get_object_str(p[1])
            pair_size = LogReporter._get_args_pair_size((p[0], p1))
            if pair_size > remaining_size:
                if remaining_size < len(p[0]):
                    break

                result[p[0]] = truncate_string(p1, remaining_size - len(p[0]), cut_in_middle=False)
                break
            remaining_size -= pair_size
            result[p[0]] = p1
        return result, max_size if max_size < 0 else remaining_size

    def _args_as_mapping(self, args: tuple[object, ...] | typing.Mapping[str, object] | None) -> dict[str, str]:
        result, remaining_size = self._make_labels(self._labels.items(), self._max_labels_size)
        if not isinstance(args, typing.Mapping):
            return result

        args_labels, _ = self._make_labels(sorted(args.items(), key=lambda p: len(repr(p[1]))), remaining_size)
        result.update(args_labels)
        return result

    def prepare_log_record(self, item: logging.LogRecord) -> LogMessage:
        labels = self._args_as_mapping(item.args)
        labels.update(self._labels)
        r = logging.LogRecord(
            name=item.name,
            level=item.levelno,
            pathname=item.pathname,
            lineno=item.lineno,
            msg=truncate_string(item.getMessage(), self._max_message_length),
            args=(labels,),
            exc_info=item.exc_info,
            func=item.funcName,
            sinfo=item.stack_info,
        )
        return LogMessage(r, labels)


def make_log_reporter(
    logger: logging.Logger,
    config: UltaConfig,
    agent: AgentInfo,
    transport_factory: ClientFactory,
    cached_logs: Queue | None = None,
) -> Reporter | NullReporter:
    if not config.log_group_id or not agent.id:
        return NullReporter()

    handler = create_sink_handler(max_queue_size=20_000)
    logger.addHandler(handler)

    client = transport_factory.create_logging_client()
    reporter = LogReporter(
        log_group_id=config.log_group_id,
        agent_id=agent.id,
        client=client,
        additional_labels={'agent_id': agent.id, 'agent_name': agent.name, 'agent_version': agent.version},
    )

    if cached_logs is not None:
        try:
            while True:
                handler.sink.put_nowait(cached_logs.get_nowait())
        except (Full, Empty):
            pass

    def error_handler(e: Exception):
        logger.warning('Failed to send logs to %s: %s', reporter._log_group_id, e)

    max_batch_size = config.log_max_chunk_size or 1000
    return Reporter(
        handler.sink,
        logger=logger,
        handler=reporter.handle,
        error_handler=error_handler,
        retention_period=config.log_retention_period or timedelta(hours=3),
        max_batch_size=max_batch_size,
        report_interval=5,
        max_unsent_size=1_000_000 // max_batch_size,
        prepare_message=reporter.prepare_log_record,
    )


def make_events_reporter(
    event_logger: logging.Logger, config: UltaConfig, agent: AgentInfo, transport_factory: ClientFactory
):
    if not agent.id:
        return NullReporter()

    client = transport_factory.create_events_log_client(agent)
    reporter = LogReporter(
        log_group_id='log_events',
        agent_id=agent.id,
        client=client,
        additional_labels={'agent_id': agent.id, 'agent_name': agent.name, 'agent_version': agent.version},
        max_labels_size=8192,
        max_message_length=2000,
    )
    handler = create_sink_handler(max_queue_size=20_000)
    event_logger.addHandler(handler)

    max_batch_size = 20_000

    def error_handler(e: Exception):
        event_logger.warning('Failed to send event logs to loadtesting: %s', e, exc_info=True)

    return Reporter(
        handler.sink,
        logger=event_logger,
        handler=reporter.handle,
        error_handler=error_handler,
        retention_period=config.log_retention_period or timedelta(hours=3),
        max_batch_size=max_batch_size,
        report_interval=5,
        max_unsent_size=1_000_000 // max_batch_size,
        prepare_message=reporter.prepare_log_record,
    )
