"""Small fixtures shared by reliability tests; no real Kafka connections."""

import json
from uuid import uuid4

from confluent_kafka import TopicPartition

from consumer.storage import KafkaRecord


def make_event(event_type="onboarding_started"):
    properties = {
        "onboarding_started": {"campaign_id": "CMP_001", "entry_point": "home_banner"},
        "profile_submitted": {"country": "SG", "occupation_group": "professional"},
        "kyc_submitted": {"kyc_method": "document_selfie"},
        "kyc_verification_completed": {"status": "passed", "review_type": "automated"},
        "account_opened": {"account_id": str(uuid4())},
        "card_activated": {"card_id": str(uuid4()), "card_type": "debit card"},
        "first_transaction_completed": {
            "transaction_id": str(uuid4()), "amount": 30.0,
            "currency": "USD", "transaction_type": "card_purchase",
        },
    }
    return {
        "event_id": str(uuid4()), "customer_id": "C_10001", "session_id": "S_test_001",
        "event_type": event_type, "event_time": "2026-11-06T00:00:00+00:00",
        "channel": "mobile_app", "device_type": "ios", "properties": properties[event_type],
    }


def record(event=None, *, offset=0, value=None):
    event = event if event is not None else make_event()
    payload = value if value is not None else json.dumps(event).encode("utf-8")
    return KafkaRecord("test-topic", 0, offset, payload, event["customer_id"].encode("utf-8"))


class FakeMessage:
    def __init__(self, source):
        self.source = source

    def topic(self):
        return self.source.topic

    def partition(self):
        return self.source.partition

    def offset(self):
        return self.source.offset

    def key(self):
        return self.source.key

    def value(self):
        return self.source.value

    def error(self):
        return None


class FakeConsumer:
    def __init__(self, records):
        self.messages = iter(FakeMessage(item) for item in records)
        self.commits = []

    def poll(self, timeout):
        try:
            return next(self.messages)
        except StopIteration:
            raise KeyboardInterrupt

    def commit(self, *, message, asynchronous):
        self.commits.append(message.offset())
        return [TopicPartition(message.topic(), message.partition(), message.offset() + 1)]


class FakeProducer:
    def __init__(self, *, delivery_error=None, queue_full_once=False):
        self.delivery_error = delivery_error
        self.queue_full_once = queue_full_once
        self.callbacks = []
        self.messages = []

    def produce(self, topic, *, key, value, on_delivery):
        if self.queue_full_once:
            self.queue_full_once = False
            raise BufferError("queue full")
        self.messages.append((topic, key, value))
        self.callbacks.append(on_delivery)

    def poll(self, timeout):
        while self.callbacks:
            self.callbacks.pop(0)(self.delivery_error, None)

    def flush(self, timeout):
        self.poll(0)
        return 0
