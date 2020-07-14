"""Module containing utilities for logging, along with a standard logger."""

import logging
from typing import Any, Optional


def _get_logger(name: Optional[str] = "outrun") -> logging.Logger:
    stderrOutput = logging.StreamHandler()

    # Explicitly emit a carriage return to help properly combine output.
    stderrOutput.terminator = "\r\n"

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    stderrOutput.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.addHandler(stderrOutput)

    return logger


def summarize(obj: Any, max_length: int = 255) -> str:
    """Return a stringified representation of the object up to the given length."""
    stringified_obj = str(obj)

    if len(stringified_obj) <= max_length:
        return stringified_obj
    else:
        return stringified_obj[: max_length - 3] + "..."


# Default logger
log = _get_logger()
