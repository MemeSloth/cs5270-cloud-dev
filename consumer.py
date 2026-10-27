"""Poll S3 for widget requests and store created widgets in S3 or DynamoDB."""

from __future__ import annotations

import argparse
import json
import logging
import math
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import Any, Callable, Mapping, Protocol


DEFAULT_REGION = "us-east-1"
DEFAULT_REQUEST_BUCKET = "usu-cs5270-golf-requests"
DEFAULT_WIDGET_BUCKET = "usu-cs5270-golf-web"
DEFAULT_TABLE_NAME = "widgets"


class RequestSource(Protocol):
    def get_next_request(self) -> Mapping[str, Any] | None:
        """Return one request, or None when no request is currently available."""


class WidgetStorage(Protocol):
    def store(self, widget: Widget) -> None:
        """Persist a widget."""


@dataclass(frozen=True)
class Widget:
    widget_id: str
    owner: str
    label: str | None
    description: str | None
    other_attributes: Mapping[str, str]

    @classmethod
    def from_create_request(cls, request: Mapping[str, Any]) -> Widget:
        request_id = request.get("requestId")
        widget_id = request.get("widgetId")
        owner = request.get("owner")
        if not isinstance(request_id, str):
            raise ValueError("Create requests must include a string requestId field")
        if not isinstance(widget_id, str) or not isinstance(owner, str):
            raise ValueError("Create requests must include string widgetId and owner fields")

        label = request.get("label")
        description = request.get("description")
        if "label" in request and not isinstance(label, str):
            raise ValueError("Create request label must be a string")
        if "description" in request and not isinstance(description, str):
            raise ValueError("Create request description must be a string")

        raw_attributes = request.get("otherAttributes", [])
        if not isinstance(raw_attributes, list):
            raise ValueError("Create request otherAttributes must be a list")

        attributes: dict[str, str] = {}
        reserved_names = {
            "widgetId",
            "widget_id",
            "owner",
            "label",
            "description",
        }
        for attribute in raw_attributes:
            if not isinstance(attribute, Mapping):
                raise ValueError("Each otherAttributes item must be an object")
            name = attribute.get("name")
            value = attribute.get("value")
            if not isinstance(name, str) or not isinstance(value, str):
                raise ValueError("Each otherAttributes item must have string name and value")
            if name in reserved_names:
                raise ValueError(f"Other attribute name conflicts with a widget field: {name}")
            if name in attributes:
                raise ValueError(f"Duplicate other attribute name: {name}")
            attributes[name] = value

        return cls(widget_id, owner, label, description, attributes)

    def to_s3_document(self) -> dict[str, str]:
        document = {"widgetId": self.widget_id, "owner": self.owner}
        if self.label is not None:
            document["label"] = self.label
        if self.description is not None:
            document["description"] = self.description
        document.update(self.other_attributes)
        return document

    def to_dynamodb_item(self) -> dict[str, str | None]:
        item: dict[str, str | None] = {
            "widget_id": self.widget_id,
            "owner": self.owner,
            "label": self.label,
            "description": self.description,
        }
        item.update(self.other_attributes)
        return item


class S3RequestSource:
    def __init__(self, s3_client: Any, bucket_name: str) -> None:
        self._s3 = s3_client
        self._bucket_name = bucket_name

    def get_next_request(self) -> Mapping[str, Any] | None:
        response = self._s3.list_objects_v2(Bucket=self._bucket_name, MaxKeys=1)
        objects = response.get("Contents", [])
        if not objects:
            return None

        key = objects[0]["Key"]
        object_response = self._s3.get_object(Bucket=self._bucket_name, Key=key)
        body = object_response["Body"]
        try:
            payload = body.read()
        finally:
            body.close()
        self._s3.delete_object(Bucket=self._bucket_name, Key=key)

        request = json.loads(payload)
        if not isinstance(request, dict):
            raise ValueError(f"Widget request in {key!r} must be a JSON object")
        return request


class S3WidgetStorage:
    def __init__(self, s3_client: Any, bucket_name: str) -> None:
        self._s3 = s3_client
        self._bucket_name = bucket_name

    def store(self, widget: Widget) -> None:
        owner_key = widget.owner.replace(" ", "-").lower()
        key = f"widgets/{owner_key}/{widget.widget_id}"
        body = json.dumps(widget.to_s3_document(), ensure_ascii=False)
        self._s3.put_object(
            Bucket=self._bucket_name,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/json",
        )


class DynamoDBWidgetStorage:
    def __init__(self, table: Any) -> None:
        self._table = table

    def store(self, widget: Widget) -> None:
        self._table.put_item(Item=widget.to_dynamodb_item())


class UnsupportedRequestTypeError(ValueError):
    pass


class WidgetRequestProcessor:
    _CREATE_TYPES = {"create", "widgetcreate", "widgetcreaterequest"}

    def __init__(self, storage: WidgetStorage) -> None:
        self._storage = storage

    def process(self, request: Mapping[str, Any]) -> None:
        request_type = request.get("type")
        if not isinstance(request_type, str):
            raise ValueError("Widget request must include a string type")

        normalized_type = request_type.casefold()
        if normalized_type not in self._CREATE_TYPES:
            raise UnsupportedRequestTypeError(
                f"Request type {request_type!r} is not implemented; HW6 supports create requests"
            )

        widget = Widget.from_create_request(request)
        self._storage.store(widget)


class Consumer:
    def __init__(
        self,
        request_source: RequestSource,
        processor: WidgetRequestProcessor,
        poll_interval_seconds: float = 0.1,
        sleep: Callable[[float], None] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        if poll_interval_seconds < 0:
            raise ValueError("Poll interval cannot be negative")
        self._request_source = request_source
        self._processor = processor
        self._poll_interval_seconds = poll_interval_seconds
        self._sleep = sleep or time.sleep
        self._logger = logger or logging.getLogger(__name__)

    def run(self, max_requests: int | None = None, stop_event: Event | None = None) -> int:
        if max_requests is not None and max_requests < 1:
            raise ValueError("max_requests must be greater than zero")

        consumed = 0
        while stop_event is None or not stop_event.is_set():
            try:
                request = self._request_source.get_next_request()
            except Exception:
                self._logger.exception("Could not retrieve a widget request")
                self._sleep(self._poll_interval_seconds)
                continue

            if request is None:
                self._sleep(self._poll_interval_seconds)
                continue

            consumed += 1
            try:
                self._processor.process(request)
                self._logger.info(
                    "Processed widget request request_id=%s",
                    request.get("requestId", "<missing>"),
                )
            except UnsupportedRequestTypeError as error:
                self._logger.warning("%s", error)
            except Exception:
                self._logger.exception("Failed to process widget request")

            if max_requests is not None and consumed >= max_requests:
                break

        return consumed


def _positive_integer(value: str) -> int:
    try:
        number = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an integer greater than zero") from error
    if number < 1:
        raise argparse.ArgumentTypeError("must be an integer greater than zero")
    return number


def _non_negative_finite_float(value: str) -> float:
    try:
        number = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a finite number greater than or equal to zero") from error
    if not math.isfinite(number) or number < 0:
        raise argparse.ArgumentTypeError("must be a finite number greater than or equal to zero")
    return number


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Consume widget requests from an S3 bucket.")
    parser.add_argument(
        "--storage",
        "--storage-strategy",
        dest="storage",
        choices=("s3", "dynamodb"),
        required=True,
        help="Where created widgets should be stored",
    )
    parser.add_argument("--request-bucket", default=DEFAULT_REQUEST_BUCKET)
    parser.add_argument("--widget-bucket", default=DEFAULT_WIDGET_BUCKET)
    parser.add_argument("--table-name", default=DEFAULT_TABLE_NAME)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--profile", help="AWS shared-credentials profile to use")
    parser.add_argument("--poll-interval-ms", type=_non_negative_finite_float, default=100.0)
    parser.add_argument("--max-requests", type=_positive_integer)
    parser.add_argument("--log-file", default="consumer.log")
    return parser


def configure_logging(log_file: str | Path) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=(
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ),
        force=True,
    )


def create_consumer(arguments: argparse.Namespace) -> Consumer:
    try:
        import boto3
    except ImportError as error:
        raise RuntimeError(
            "boto3 is required to access AWS; install dependencies with "
            "'python -m pip install -r requirements.txt'"
        ) from error

    session = boto3.Session(
        profile_name=arguments.profile,
        region_name=arguments.region,
    )
    s3_client = session.client("s3")
    request_source = S3RequestSource(s3_client, arguments.request_bucket)

    if arguments.storage == "s3":
        storage: WidgetStorage = S3WidgetStorage(s3_client, arguments.widget_bucket)
    else:
        table = session.resource("dynamodb").Table(arguments.table_name)
        storage = DynamoDBWidgetStorage(table)

    processor = WidgetRequestProcessor(storage)
    return Consumer(
        request_source,
        processor,
        poll_interval_seconds=arguments.poll_interval_ms / 1000,
    )


def main(argv: list[str] | None = None) -> int:
    arguments = build_argument_parser().parse_args(argv)
    configure_logging(arguments.log_file)
    logger = logging.getLogger(__name__)

    try:
        consumer = create_consumer(arguments)
        logger.info(
            "Starting consumer: storage=%s request_bucket=%s region=%s",
            arguments.storage,
            arguments.request_bucket,
            arguments.region,
        )
        consumer.run(max_requests=arguments.max_requests)
    except KeyboardInterrupt:
        logger.info("Consumer stopped")
    except Exception:
        logger.exception("Consumer could not start or stopped unexpectedly")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
