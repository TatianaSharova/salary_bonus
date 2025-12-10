import logging as _logging

_logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s",
    level=_logging.INFO,
)

logging = _logging.getLogger(__name__)
