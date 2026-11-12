import copy
import json
import unittest

from consumer.validation import EVENT_TYPES, EventValidationError, validate_event
from tests.helpers import make_event


class ValidationTests(unittest.TestCase):
    def test_all_supported_stages_and_extra_fields_are_preserved(self):
        for stage in EVENT_TYPES:
            with self.subTest(stage=stage):
                event = make_event(stage)
                event["extra_context"] = {"source": "fixture"}
                result = validate_event(json.dumps(event).encode(), b"C_10001")
                self.assertEqual(result.data, event)
                self.assertEqual(json.loads(result.canonical_json), event)

    def test_malformed_messages_are_rejected(self):
        cases = [
            (None, "null_payload"), (b"\xff", "invalid_utf8"),
            (b"{broken", "invalid_json"), (b"[]", "invalid_object"),
            (b"null", "invalid_object"), (b'{"a":1,"a":2}', "duplicate_json_key"),
            (b'{"a":NaN}', "invalid_json_number"), (b"1" * 5000, "invalid_json"),
        ]
        for payload, code in cases:
            with self.subTest(payload=str(payload)[:30]), self.assertRaises(EventValidationError) as caught:
                validate_event(payload)
            self.assertEqual(caught.exception.code, code)

    def test_invalid_fields_are_rejected(self):
        changes = [
            ("event_id", "not-a-uuid"), ("session_id", ""), ("customer_id", None),
            ("event_type", "unknown"), ("channel", "email"), ("device_type", "desktop"),
            ("event_time", "2026-11-06T00:00:00"), ("event_time", "not-a-date"),
            ("properties", []),
        ]
        for field, value in changes:
            with self.subTest(field=field):
                event = make_event()
                event[field] = value
                with self.assertRaises(EventValidationError):
                    validate_event(json.dumps(event).encode())
        event = make_event()
        del event["session_id"]
        with self.assertRaisesRegex(EventValidationError, "session_id"):
            validate_event(json.dumps(event).encode())

    def test_stage_properties_and_key_are_checked(self):
        for stage, field, value in [
            ("profile_submitted", "country", "singapore"),
            ("kyc_verification_completed", "status", "pending"),
            ("account_opened", "account_id", "invalid"),
            ("first_transaction_completed", "currency", "usd"),
        ]:
            event = make_event(stage)
            event["properties"][field] = value
            with self.subTest(stage=stage), self.assertRaises(EventValidationError):
                validate_event(json.dumps(event).encode())
        for amount in (True, 0, -1, "30", float("inf")):
            event = make_event("first_transaction_completed")
            event["properties"]["amount"] = amount
            with self.subTest(amount=amount), self.assertRaises(EventValidationError):
                validate_event(json.dumps(event).encode())
        for key in (b"different-customer", b"\xff"):
            with self.subTest(key=key), self.assertRaises(EventValidationError):
                validate_event(json.dumps(make_event()).encode(), key)

    def test_normalization_handles_key_order_uuid_case_and_timezone(self):
        event = make_event()
        equivalent = copy.deepcopy(event)
        equivalent["event_id"] = event["event_id"].upper()
        equivalent["event_time"] = "2026-11-06T08:00:00+08:00"
        original = validate_event(json.dumps(event).encode())
        normalized = validate_event(json.dumps(equivalent, sort_keys=True, indent=2).encode())
        self.assertEqual(original.payload_hash, normalized.payload_hash)

    def test_non_finite_numbers_and_unpaired_unicode_are_rejected(self):
        event = make_event()
        event["extra"] = "\ud800"
        with self.assertRaises(EventValidationError):
            validate_event(json.dumps(event).encode())
        raw = json.dumps(make_event()).replace('"properties":', '"extra": 1e999, "properties":')
        with self.assertRaises(EventValidationError):
            validate_event(raw.encode())
