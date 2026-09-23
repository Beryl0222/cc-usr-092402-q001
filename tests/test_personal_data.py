"""个人材料撤回测试。

撤回只清空证据正文，元数据与所支撑判罚保留，案件仍可追溯；
重复撤回幂等；不含个人材料的证据不可撤回；撤回不影响金额守恒。
"""

from __future__ import annotations

import tempfile
import unittest

from src.jointcase.service import AuthorizationError

from tests._helpers import build_two_item_case


class PersonalDataRedactionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ctx = build_two_item_case(self.tmp.name, case_id="C-GDPR")
        self.svc = self.ctx["svc"]
        self.store = self.ctx["store"]
        self.cid = "C-GDPR"

    def tearDown(self):
        self.tmp.cleanup()

    def test_redaction_clears_content_but_keeps_metadata_and_decisions(self):
        case = self.store.load_case(self.cid)
        evidence = case.evidences["E-MERCHANT"]
        self.assertTrue(evidence.contains_personal_data)
        self.assertIsNotNone(evidence.content)

        self.svc.redact_personal_data(self.cid, actor="agent", evidence_id="E-MERCHANT")
        case = self.store.load_case(self.cid)
        evidence = case.evidences["E-MERCHANT"]
        self.assertIsNone(evidence.content)
        self.assertTrue(evidence.redacted)
        self.assertIsNotNone(evidence.redacted_at)
        # 元数据保留
        self.assertEqual(evidence.source_party, "merchant")
        self.assertEqual(evidence.kind, "MERCHANT_COMMITMENT")
        # 所支撑判罚依然完整可追溯
        decision = case.current_decision("I1")
        self.assertIn("E-MERCHANT", decision.basis_evidence)
        self.assertEqual(decision.shares_cents, {"merchant": 3000, "organizer": 1000})

    def test_redaction_is_idempotent(self):
        seq1 = self.store.load_case(self.cid).seq
        self.svc.redact_personal_data(self.cid, actor="agent", evidence_id="E-MERCHANT")
        seq2 = self.store.load_case(self.cid).seq
        result = self.svc.redact_personal_data(self.cid, actor="agent",
                                               evidence_id="E-MERCHANT")
        self.assertEqual(result, [])
        self.assertEqual(self.store.load_case(self.cid).seq, seq2)
        self.assertGreater(seq2, seq1)

    def test_non_personal_evidence_cannot_be_redacted(self):
        from src.jointcase.domain import RuleViolation
        with self.assertRaises(Exception) as cm:
            self.svc.redact_personal_data(self.cid, actor="agent", evidence_id="E-TX")
        # E-TX 在构造时未标记个人材料
        self.assertIsInstance(cm.exception, RuleViolation)

    def test_unauthorized_party_cannot_redact(self):
        with self.assertRaises(AuthorizationError):
            self.svc.redact_personal_data(self.cid, actor="merchant",
                                          evidence_id="E-MERCHANT")

    def test_redaction_visible_in_party_view_without_leaking_content(self):
        from src.jointcase import projections as P
        self.svc.redact_personal_data(self.cid, actor="agent", evidence_id="E-MERCHANT")
        case = self.store.load_case(self.cid)
        view = P.party_view(case, "merchant")
        ev = next(e for e in view["evidences"] if e["evidence_id"] == "E-MERCHANT")
        self.assertIsNone(ev["content"])
        self.assertTrue(ev["redacted"])


if __name__ == "__main__":
    unittest.main()
