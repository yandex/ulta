import logging
import typing
from datetime import timedelta
from ulta.common.config import UltaConfig
from ulta.common.agent import AgentInfo
from ulta.common.reporter import NullReporter, Reporter
from ulta.common.interfaces import ClientFactory, RemoteLoggingClient
from ulta.common.logging import SinkHandler, LogMessage


class LogReporter:
    def __init__(
        self,
        log_group_id: str,
        agent_id: str,
        client: RemoteLoggingClient,
        additional_labels: dict[str, str] | None = None,
    ):
        self._agent_id = agent_id
        self._log_group_id = log_group_id
        self._client = client
        self._labels = additional_labels or {}

    def handle(self, records: list[logging.LogRecord]):
        messages: list[LogMessage] = []
        for r in records:
            labels = self._args_as_mapping(r.args)
            labels.update(self._labels)
            messages.append(LogMessage(r, labels))

        self._client.send_log(
            self._log_group_id,
            messages,
            resource_type='agent_logs',
            resource_id=self._agent_id,
        )

    def _args_as_mapping(self, args: tuple[object, ...] | typing.Mapping[str, object] | None) -> dict[str, str | None]:
        if isinstance(args, typing.MutableMapping):
            return {k: str(v) for k, v in args.items()}
        # unreachable code
        return {}


def make_log_reporter(
    logger: logging.Logger, config: UltaConfig, agent: AgentInfo, transport_factory: ClientFactory
) -> Reporter | NullReporter:
    sinks = [h._sink for h in logger.handlers if isinstance(h, SinkHandler)]
    if not sinks or not config.log_group_id or not agent.id:
        for h in logger.handlers:
            if isinstance(h, SinkHandler):
                logger.removeHandler(h)
        return NullReporter()

    client = transport_factory.create_logging_client()
    reporter = LogReporter(
        log_group_id=config.log_group_id,
        agent_id=agent.id,
        client=client,
    )

    def error_handler(e: Exception):
        logger.warning('Failed to send logs to %s: %s', reporter._log_group_id, e)

    max_batch_size = config.log_max_chunk_size or 1000
    return Reporter(
        *sinks,
        logger=logger,
        handler=reporter.handle,
        error_handler=error_handler,
        retention_period=config.log_retention_period or timedelta(hours=3),
        max_batch_size=config.log_max_chunk_size or 1000,
        report_interval=5,
        max_unsent_size=1_000_000 // max_batch_size,
    )
