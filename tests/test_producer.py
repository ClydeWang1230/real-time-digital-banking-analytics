import unittest
from unittest.mock import Mock, patch

from config import load_settings
import producer.producer as publishing
from tests.helpers import FakeProducer


class ProducerTests(unittest.TestCase):
    def test_success_requires_delivery_callbacks(self):
        producer = FakeProducer()
        stats = publishing.publish_records([(b"key", b"value")], load_settings({}), producer=producer)
        self.assertEqual((stats.attempted, stats.queued, stats.acknowledged, stats.failed, stats.pending), (1, 1, 1, 0, 0))
        options = publishing.producer_config(load_settings({}))
        self.assertIs(options["enable.idempotence"], True)
        self.assertEqual(options["acks"], "all")

    def test_empty_queue_with_failed_delivery_is_not_success(self):
        producer = FakeProducer(delivery_error="timed out")
        with self.assertLogs(publishing.LOGGER, level="ERROR"), self.assertRaises(publishing.DeliveryError) as caught:
            publishing.publish_records([(b"key", b"value")], load_settings({}), producer=producer)
        self.assertEqual(caught.exception.stats.failed, 1)
        self.assertEqual(caught.exception.stats.pending, 0)

    def test_missing_callback_or_remaining_queue_fails(self):
        for pending in (0, 1):
            producer = Mock()
            producer.flush.return_value = pending
            with self.subTest(pending=pending), self.assertRaises(publishing.DeliveryError):
                publishing.publish_records([(b"key", b"value")], load_settings({}), producer=producer)

    def test_queue_full_can_recover_without_losing_a_record(self):
        producer = FakeProducer(queue_full_once=True)
        stats = publishing.publish_records([(b"key", b"value")], load_settings({}), producer=producer)
        self.assertEqual((stats.queued, stats.acknowledged, len(producer.messages)), (1, 1, 1))

    def test_permanently_full_queue_has_a_bounded_wait(self):
        producer = Mock()
        producer.produce.side_effect = BufferError("full")
        producer.flush.return_value = 0
        with patch.object(publishing.time, "monotonic", side_effect=[0, 100]), self.assertRaises(publishing.DeliveryError) as caught:
            publishing.publish_records([(b"key", b"value")], load_settings({}), producer=producer)
        self.assertEqual((caught.exception.stats.attempted, caught.exception.stats.queued), (1, 0))

    def test_generator_failure_is_reported_after_flushing_queued_records(self):
        def broken_source():
            yield b"key", b"value"
            raise ValueError("generation failed")

        with self.assertRaises(publishing.DeliveryError) as caught:
            publishing.publish_records(broken_source(), load_settings({}), producer=FakeProducer())
        self.assertEqual(caught.exception.stats.acknowledged, 1)
        self.assertIn("generation failed", str(caught.exception))
