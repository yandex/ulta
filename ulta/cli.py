import sys

from ulta.common.logging import init_logging
from ulta.config import configure, Command
from ulta.service.command import run_serve
from ulta.version import VERSION

import ulta.yc  # noqa: ulta.yc is the default plugin for Yandex.Cloud Loadtesting backend.


def main():
    config, explanation = configure()
    logger = init_logging(config)
    exit_code = 0

    try:
        if config.command == Command.SERVE or (config.command == Command.RUN and config.test_id):
            exit_code = run_serve(config, explanation, logger)
        elif config.command == Command.VERSION:
            print(VERSION)
        else:
            exit_code = 'Invalid arguments specified. See `ulta --help` for usage'
    except Exception:
        logger.exception('Ulta execution failure')
        exit_code = 1
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
