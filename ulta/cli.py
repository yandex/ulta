import logging
import signal
import sys

from ulta.common.cancellation import Cancellation, CancellationType
from ulta.common.config import UltaConfig
from ulta.common.interfaces import TransportFactory, ClientFactory
from ulta.config import configure, Command
from ulta.logging import create_default_logger
from ulta.module import load_class
from ulta.service.command import run_serve
from ulta.service.tank_client import TankClient
from ulta.version import VERSION
from yandextank.contrib.netort.netort.resource import ResourceManager

import ulta.yc  # noqa: ulta.yc is the default plugin for Yandex.Cloud Loadtesting backend.


def main():
    config, explanation = configure()

    logger = create_default_logger(config)
    logger.info('ulta config %s', explanation)

    cancellation = setup_cancellation(logger)

    try:
        setup_plugins(config, logger)

        if config.command == Command.SERVE or (config.command == Command.RUN and config.test_id):
            sys.exit(run_serve(config, cancellation, logger))
        elif config.command == Command.VERSION:
            print(VERSION)
        else:
            sys.exit('Invalid arguments specified. See `ulta --help` for usage')
    except Exception:
        logger.exception('Ulta execution failure')
        sys.exit(1)


def setup_cancellation(logger: logging.Logger) -> Cancellation:
    cancellation = Cancellation()

    def terminate(signo, *args):
        cancellation.notify(f'Received signal: {signal.Signals(signo).name}')
        if cancellation.is_set(CancellationType.FORCED):
            logger.warning('Received signal: %s. Terminating service', signal.Signals(signo).name)
        elif cancellation.is_set(CancellationType.GRACEFUL):
            logger.warning(
                'Received signal: %s. Awaiting current job to finish and terminating...', signal.Signals(signo).name
            )
        else:
            logger.warning('Received signal: %s. Terminating...', signal.Signals(signo).name)

    signal.signal(signal.SIGINT, terminate)
    signal.signal(signal.SIGTERM, terminate)

    return cancellation


def setup_plugins(config: UltaConfig, logger: logging.Logger):
    # setup transport factory
    if config.transport:
        logger.info('Using transport factory %s', config.transport)
        TransportFactory.use(load_class(config.transport, base_class=ClientFactory))

    if config.netort_resource_manager:
        logger.info('Using netort resource manager %s', config.netort_resource_manager)
        resource_manager = load_class(config.netort_resource_manager, base_class=ResourceManager)
        TankClient.use_resource_manager(resource_manager)
