"""Unit tests for retry policy validation and retry-record serialization."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from atmo_sync.retry import RetryEnvelope, RetryPolicy, TransientProcessingError


class RetryPolicyTests(unittest.TestCase):
    def test_exponential_backoff_is_capped_and_jitter_is_configurable(self):
        policy = RetryPolicy(
            max_attempts=5,
            base_delay_seconds=2,
            max_delay_seconds=5,
            jitter_fraction=0.25,
        )
        self.assertEqual(
            policy.delay_for_attempt(1, random_uniform=lambda low, _high: low),
            1.5,
        )
        self.assertEqual(
            policy.delay_for_attempt(2, random_uniform=lambda _low, high: high),
            5,
        )
        self.assertEqual(
            policy.delay_for_attempt(5, random_uniform=lambda _low, high: high),
            5,
        )

    def test_invalid_retry_policy_values_are_rejected(self):
        for kwargs in (
            {"max_attempts": -1},
            {"base_delay_seconds": float("nan")},
            {"max_delay_seconds": 0},
            {"base_delay_seconds": 2, "max_delay_seconds": 1},
            {"jitter_fraction": 1.1},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                RetryPolicy(**kwargs)

    def test_retry_envelope_round_trip_preserves_original_bytes_and_metadata(self):
        payload = b"\x00original sensor bytes\xff"
        source_key = b"original-key"
        scheduled = datetime.now(timezone.utc) + timedelta(seconds=3)
        timestamp = scheduled.isoformat(timespec="milliseconds").replace(
            "+00:00", "Z"
        )
        retry = RetryEnvelope(
            event_id="event-123",
            source_topic="sensor-events",
            source_partition=0,
            source_offset=42,
            source_key=source_key,
            raw_payload=payload,
            attempt_number=2,
            first_failure_at=timestamp,
            last_failure_at=timestamp,
            failure_reason="temporary processing error",
            next_attempt_at=timestamp,
            retry_history=(
                {
                    "attempt_number": 1,
                    "failed_at": timestamp,
                    "failure_reason": "first failure",
                    "retry_delay_seconds": 1.5,
                    "next_attempt_at": timestamp,
                },
            ),
        )

        decoded = RetryEnvelope.from_bytes(retry.to_bytes())

        self.assertEqual(decoded, retry)
        self.assertEqual(decoded.raw_payload, payload)
        self.assertEqual(decoded.source_key, source_key)

    def test_malformed_retry_envelope_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "valid UTF-8 JSON"):
            RetryEnvelope.from_bytes(b"\xff")
        with self.assertRaisesRegex(ValueError, "attempt_number"):
            RetryEnvelope.from_bytes(
                b'{"schema_version":"1.0","attempt_number":0}'
            )

    def test_transient_error_has_distinct_processing_classification(self):
        error = TransientProcessingError("temporary service failure")
        self.assertIsInstance(error, Exception)
        self.assertEqual(str(error), "temporary service failure")


if __name__ == "__main__":
    unittest.main()
