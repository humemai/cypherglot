from __future__ import annotations

import logging


LOGGER_NAME = "cypherglot"


_package_logger = logging.getLogger(LOGGER_NAME)
if not any(isinstance(handler, logging.NullHandler) for handler in _package_logger.handlers):
    _package_logger.addHandler(logging.NullHandler())


def get_logger(module_name: str) -> logging.Logger:
    if module_name == LOGGER_NAME or module_name.startswith(f"{LOGGER_NAME}."):
        return logging.getLogger(module_name)
    return logging.getLogger(f"{LOGGER_NAME}.{module_name.rsplit('.', 1)[-1]}")