"""Taegis Magic application logging."""

import logging
import functools
from typing import Callable


TRACE_LOG_LEVEL = 5
logging.addLevelName(TRACE_LOG_LEVEL, "TRACE")


def trace(self: logging.Logger, message: str, *args, **kwargs):
    """
    Trace log level for function decorator.

    Parameters
    ----------
    self : logging.Logger
        Logger
    message : _type_
        Message to log
    """
    if self.isEnabledFor(TRACE_LOG_LEVEL):
        self._log(TRACE_LOG_LEVEL, message, args, **kwargs)


logging.Logger.trace = trace

log = logging.getLogger(__name__)


def get_module_logger() -> logging.Logger:
    """
    Get the module logger object.

    Returns
    -------
    logging.Logger
        Taegis Magic module logger
    """
    logger = logging.getLogger("taegis_magic")
    logger.propagate = False
    logger.handlers = []

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s::%(levelname)s::%(name)s::%(message)s", datefmt="%Y-%m-%dT%H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def tracing(func: Callable):
    """
    Log entry and exit from function.

    Parameters
    ----------
    func : Callable
        Function to log
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        log.trace(f"Entering {func.__name__}(args: {args}, kwargs: {kwargs})...")

        try:
            result = func(*args, **kwargs)
        except Exception as exc:
            log.error(f"Exception raised in {func.__name__}. exception: {str(exc)}")
            raise exc

        log.trace(f"Exiting {func.__name__}...")

        return result

    return wrapper
