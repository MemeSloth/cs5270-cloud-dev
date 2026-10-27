import io
import json
import logging
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from threading import Event
from unittest.mock import Mock

from consumer import (
    Consumer,
    DynamoDBWidgetStorage,
    S3RequestSource,
    S3WidgetStorage,
    UnsupportedRequestTypeError,
    Widget,
    WidgetRequestProcessor,
    build_argument_parser,
    configure_logging,
)


def create_request(**changes):
    request = {
        "type": "create",
        "requestId": "request-1",
        "widgetId": "widget-1",
        "owner": "Mary Matthews",
        "label": "sample",
        "description": "test widget",
        "otherAttributes": [
            {"name": "width-unit", "value": "cm"},
            {"name": "rating", "value": "2.5"},
        ],
    }
    request.update(changes)
    return request


class WidgetTests(unittest.TestCase):
    def test_create_request_maps_core_and_flat_attributes(self):
        widget = Widget.from_create_request(create_request())

        self.assertEqual(widget.widget_id, "widget-1")
        self.assertEqual(widget.other_attributes, {"width-unit": "cm", "rating": "2.5"})
        self.assertEqual(
            widget.to_s3_document(),
            {
                "widgetId": "widget-1",
                "owner": "Mary Matthews",
                "label": "sample",
                "description": "test widget",
                "width-unit": "cm",
                "rating": "2.5",
            },
        )
        self.assertEqual(
            widget.to_dynamodb_item(),
            {
                "widget_id": "widget-1",
                "owner": "Mary Matthews",
                "label": "sample",
                "description": "test widget",
                "width-unit": "cm",
                "rating": "2.5",
            },
        )

    def test_rejects_duplicate_or_core_field_other_attributes(self):
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            Widget.from_create_request(
                create_request(
                    otherAttributes=[
                        {"name": "size", "value": "small"},
                        {"name": "size", "value": "large"},
                    ]
                )
            )

        with self.assertRaisesRegex(ValueError, "conflicts"):
            Widget.from_create_request(
                create_request(otherAttributes=[{"name": "owner", "value": "Someone"}])
            )

    def test_optional_core_attributes_may_be_absent(self):
        request = create_request(otherAttributes=[])
        request.pop("label")
        request.pop("description")
        widget = Widget.from_create_request(request)

        self.assertEqual(widget.to_s3_document(), {"widgetId": "widget-1", "owner": "Mary Matthews"})
        self.assertIsNone(widget.to_dynamodb_item()["label"])

    def test_create_request_requires_schema_fields_and_rejects_null_strings(self):
        with self.assertRaisesRegex(ValueError, "requestId"):
            Widget.from_create_request(create_request(requestId=None))
        with self.assertRaisesRegex(ValueError, "label"):
            Widget.from_create_request(create_request(label=None))


class S3RequestSourceTests(unittest.TestCase):
    def test_reads_and_deletes_single_listed_request_before_returning_it(self):
        client = Mock()
        client.list_objects_v2.return_value = {"Contents": [{"Key": "request-1"}]}
        client.get_object.return_value = {"Body": io.BytesIO(b'{"type": "create"}')}
        events = []
        client.list_objects_v2.side_effect = lambda **kwargs: (
            events.append(("list", kwargs)) or {"Contents": [{"Key": "request-1"}]}
        )
        client.get_object.side_effect = lambda **kwargs: (
            events.append(("get", kwargs))
            or {"Body": io.BytesIO(b'{"type": "create"}')}
        )
        client.delete_object.side_effect = lambda **kwargs: events.append(("delete", kwargs))
        source = S3RequestSource(client, "requests")

        request = source.get_next_request()

        self.assertEqual(request, {"type": "create"})
        self.assertEqual([event[0] for event in events], ["list", "get", "delete"])
        self.assertEqual(events[0][1], {"Bucket": "requests", "MaxKeys": 1})
        self.assertEqual(events[2][1], {"Bucket": "requests", "Key": "request-1"})

    def test_returns_none_when_bucket_has_no_request(self):
        client = Mock()
        client.list_objects_v2.return_value = {}

        self.assertIsNone(S3RequestSource(client, "requests").get_next_request())
        client.get_object.assert_not_called()
        client.delete_object.assert_not_called()

    def test_deletes_but_rejects_non_object_json(self):
        client = Mock()
        client.list_objects_v2.return_value = {"Contents": [{"Key": "request-1"}]}
        client.get_object.return_value = {"Body": io.BytesIO(b"[]")}

        with self.assertRaisesRegex(ValueError, "JSON object"):
            S3RequestSource(client, "requests").get_next_request()

        client.delete_object.assert_called_once()

    def test_deletes_and_reports_malformed_json(self):
        client = Mock()
        client.list_objects_v2.return_value = {"Contents": [{"Key": "request-1"}]}
        client.get_object.return_value = {"Body": io.BytesIO(b"")}

        with self.assertRaises(json.JSONDecodeError):
            S3RequestSource(client, "requests").get_next_request()

        client.delete_object.assert_called_once()


class StorageTests(unittest.TestCase):
    def test_s3_storage_uses_required_key_and_json_document(self):
        client = Mock()
        widget = Widget.from_create_request(create_request())

        S3WidgetStorage(client, "widgets-bucket").store(widget)

        call = client.put_object.call_args.kwargs
        self.assertEqual(call["Bucket"], "widgets-bucket")
        self.assertEqual(call["Key"], "widgets/mary-matthews/widget-1")
        self.assertEqual(call["ContentType"], "application/json")
        self.assertEqual(
            json.loads(call["Body"].decode("utf-8")),
            widget.to_s3_document(),
        )

    def test_dynamodb_storage_writes_flat_item(self):
        table = Mock()
        widget = Widget.from_create_request(create_request())

        DynamoDBWidgetStorage(table).store(widget)

        table.put_item.assert_called_once_with(Item=widget.to_dynamodb_item())
        self.assertNotIn("otherAttributes", table.put_item.call_args.kwargs["Item"])


class ProcessorTests(unittest.TestCase):
    def test_processes_create_request(self):
        storage = Mock()

        WidgetRequestProcessor(storage).process(create_request())

        stored_widget = storage.store.call_args.args[0]
        self.assertIsInstance(stored_widget, Widget)
        self.assertEqual(stored_widget.widget_id, "widget-1")

    def test_rejects_update_and_delete_until_hw7(self):
        processor = WidgetRequestProcessor(Mock())

        for request_type in ("delete", "update", "WidgetDeleteRequest", "WidgetUpdateRequest"):
            with self.subTest(request_type=request_type):
                with self.assertRaises(UnsupportedRequestTypeError):
                    processor.process(create_request(type=request_type))

    def test_accepts_schema_create_type_spelling(self):
        storage = Mock()

        WidgetRequestProcessor(storage).process(
            create_request(type="WidgetCreateRequest")
        )

        storage.store.assert_called_once()


class ConsumerTests(unittest.TestCase):
    def test_run_processes_one_request_and_stops_at_limit(self):
        source = Mock()
        source.get_next_request.return_value = create_request()
        storage = Mock()
        logger = Mock()
        consumer = Consumer(
            source,
            WidgetRequestProcessor(storage),
            sleep=Mock(),
            logger=logger,
        )

        consumed = consumer.run(max_requests=1)

        self.assertEqual(consumed, 1)
        source.get_next_request.assert_called_once()
        storage.store.assert_called_once()
        logger.info.assert_called_once_with(
            "Processed widget request request_id=%s", "request-1"
        )

    def test_empty_source_waits_between_polls(self):
        source = Mock()
        source.get_next_request.return_value = None
        sleep = Mock(side_effect=lambda _: stop_event.set())
        stop_event = Event()
        consumer = Consumer(
            source,
            WidgetRequestProcessor(Mock()),
            poll_interval_seconds=0.1,
            sleep=sleep,
        )

        self.assertEqual(consumer.run(stop_event=stop_event), 0)
        sleep.assert_called_once_with(0.1)
        source.get_next_request.assert_called_once()

    def test_logs_processing_failure_and_counts_consumed_request(self):
        source = Mock()
        source.get_next_request.return_value = {"type": "create"}
        logger = Mock()
        consumer = Consumer(
            source,
            WidgetRequestProcessor(Mock()),
            logger=logger,
        )

        self.assertEqual(consumer.run(max_requests=1), 1)
        logger.exception.assert_called_once_with("Failed to process widget request")

    def test_warns_for_requests_reserved_for_hw7(self):
        source = Mock()
        source.get_next_request.return_value = create_request(type="delete")
        logger = Mock()
        consumer = Consumer(
            source,
            WidgetRequestProcessor(Mock()),
            logger=logger,
        )

        self.assertEqual(consumer.run(max_requests=1), 1)
        logger.warning.assert_called_once()


class CommandLineAndLoggingTests(unittest.TestCase):
    def test_parser_defaults_to_provided_aws_resources(self):
        arguments = build_argument_parser().parse_args(["--storage", "s3"])

        self.assertEqual(arguments.request_bucket, "usu-cs5270-golf-requests")
        self.assertEqual(arguments.widget_bucket, "usu-cs5270-golf-web")
        self.assertEqual(arguments.table_name, "widgets")
        self.assertEqual(arguments.region, "us-east-1")

    def test_parser_accepts_dynamodb_strategy_and_request_limit(self):
        arguments = build_argument_parser().parse_args(
            ["--storage-strategy", "dynamodb", "--max-requests", "3"]
        )

        self.assertEqual(arguments.storage, "dynamodb")
        self.assertEqual(arguments.max_requests, 3)

    def test_parser_rejects_invalid_poll_intervals(self):
        parser = build_argument_parser()

        for interval in ("-1", "nan", "inf"):
            with self.subTest(interval=interval), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    parser.parse_args(
                        ["--storage", "s3", "--poll-interval-ms", interval]
                    )

    def test_logging_writes_to_requested_file(self):
        with tempfile.TemporaryDirectory() as directory:
            log_path = Path(directory) / "consumer.log"
            configure_logging(log_path)
            logging.getLogger("consumer-test").info("test log entry")
            logging.shutdown()

            self.assertIn("test log entry", log_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
