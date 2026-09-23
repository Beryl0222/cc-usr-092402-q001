"""事件合同信封校验测试（原对外合同 + 联合处置内部信封）。"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from src.event_consumer_guard import (
    JOINT_EVENT_TYPES,
    validate_event,
    validate_joint_envelope,
)


class OriginalContractTest(unittest.TestCase):
    def test_sample_matches_domain_contract(self):
        record = json.loads(
            (Path(__file__).parents[1] / "data" / "sample.json").read_text(encoding="utf-8")
        )
        self.assertEqual(validate_event(record), [])

    def test_original_kinds_remain_supported(self):
        for kind in ("EVENT_PUBLISHED", "MERCHANT_COMMITMENT", "BENEFIT_REDEEMED",
                     "COMPLAINT_OPENED", "REMEDY_SETTLED"):
            self.assertEqual(
                validate_event({"event_id": "x", "kind": kind, "occurred_at": "t",
                                "subject_id": "s", "payload": {}}),
                [],
            )


class JointEnvelopeContractTest(unittest.TestCase):
    def _valid(self, **over):
        record = {
            "case_id": "C1",
            "seq": 0,
            "event_type": "CaseOpened",
            "occurred_at": "2026-09-23T09:00:00+08:00",
            "actor": "agent",
            "data": {},
        }
        record.update(over)
        return record

    def test_valid_envelope(self):
        self.assertEqual(validate_joint_envelope(self._valid()), [])

    def test_missing_fields_reported(self):
        problems = validate_joint_envelope({"case_id": "C1"})
        self.assertIn("seq", problems)
        self.assertIn("event_type", problems)
        self.assertIn("occurred_at", problems)
        self.assertIn("actor", problems)
        self.assertIn("data", problems)

    def test_unknown_event_type(self):
        self.assertIn(
            "event_type",
            validate_joint_envelope(self._valid(event_type="SomethingElse")),
        )

    def test_seq_must_be_non_negative_int(self):
        self.assertIn("seq", validate_joint_envelope(self._valid(seq=-1)))
        self.assertIn("seq", validate_joint_envelope(self._valid(seq="1")))
        self.assertIn("seq", validate_joint_envelope(self._valid(seq=True)))
        self.assertEqual(validate_joint_envelope(self._valid(seq=0)), [])
        self.assertEqual(validate_joint_envelope(self._valid(seq=42)), [])

    def test_occurred_at_must_be_parseable_aware(self):
        self.assertIn("occurred_at",
                      validate_joint_envelope(self._valid(occurred_at="not-a-time")))
        self.assertIn("occurred_at",
                      validate_joint_envelope(
                          self._valid(occurred_at="2026-09-23T09:00:00")))

    def test_all_joint_event_types_accepted(self):
        for et in JOINT_EVENT_TYPES:
            self.assertEqual(
                validate_joint_envelope(self._valid(event_type=et, seq=1)),
                [],
                et,
            )


if __name__ == "__main__":
    unittest.main()
