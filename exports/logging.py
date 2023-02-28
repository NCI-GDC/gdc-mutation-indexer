import logging
from logging import handlers
import platform
import uuid
from typing import Any, Dict

from pythonjsonlogger import jsonlogger


class DatadogLogFormatter(jsonlogger.JsonFormatter):
    def __init__(self, service_name: str) -> None:
        super().__init__(
            "%(asctime)s %(levelname)s %(name)s %(message)s",
            rename_fields={
                "levelname": "level",
                "asctime": "timestamp",
                "name": "logger.name",
            },
            static_fields={"service": service_name},
        )

    def add_fields(
        self,
        log_record: Dict[str, Any],
        record: logging.LogRecord,
        message_dict: Dict[str, Any],
    ) -> None:
        super().add_fields(log_record, record, message_dict)

        if record.exc_info:
            exc_type, exception, tb = record.exc_info
            log_record["error.kind"] = f"{exc_type.__module__}.{exc_type.__name__}"
            log_record["error.message"] = f"{exception}"
            log_record["error.stack"] = log_record.pop("exc_info", None)

        log_record["host"] = platform.node()


log_formatter = DatadogLogFormatter("mutation_indexer")


def configure() -> None:
    log_handler = handlers.WatchedFileHandler(
        "/var/log/python/mutation_indexer.json", mode="a+"
    )

    log_handler.setFormatter(log_formatter)
    logging.basicConfig(handlers=(log_handler,), level=logging.INFO, force=True)


def add_build_id(build_id: uuid.UUID) -> None:
    log_formatter.static_fields["build_id"] = build_id
