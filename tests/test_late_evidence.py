"""场景二：迟到/重复证据。

- 同一证据可能在案件不同阶段重复到达：必须幂等，只产生一次事件；
- 结案后才到达的关键证据必须让案件可重审；
- 非关键证据在结案后到达不应推翻结案；
- 证据去重以 source_party + evidence_id 为准，不同来源的同号证据各自保留。
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import timedelta

from src.jointcase import clock
from src.jointcase.store import EventStore

from tests._helpers import build_two_item_case


class LateEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ctx = build_two_item_case(self.tmp.name, case_id="C-LATE")
        self.svc = self.ctx["svc"]
        self.store = self.ctx["store"]
        self.cid = "C-LATE"
        self.deadline = self.ctx["deadline"]

    def tearDown(self):
        self.tmp.cleanup()

    def _finish_and_close(self):
        self.svc.consumer_choose(self.cid, actor="consumer", offer_id="O1",
                                 option_id="OPT-REFUND", accepted=True)
        self.svc.complete_action(self.cid, actor="merchant", action_id="A1",
                                 result_ref="pay-A1")
        self.svc.complete_action(self.cid, actor="organizer", action_id="A2",
                                 result_ref="pay-A2")
        self.svc.complete_action(self.cid, actor="platform", action_id="A3",
                                 result_ref="new-benefit")
        self.svc.close_case(self.cid, actor="agent", summary="双方动作完成")

    def test_duplicate_evidence_is_idempotent(self):
        before = self.store.load_case(self.cid).seq
        ev1 = self.svc.submit_evidence(self.cid, actor="agent", evidence_id="E-DUP",
                                       source_party="merchant", kind="MERCHANT_COMMITMENT",
                                       content={"v": 1})
        after_first = self.store.load_case(self.cid).seq
        # 内容不同也不能覆盖：以首次到达为准
        ev2 = self.svc.submit_evidence(self.cid, actor="agent", evidence_id="E-DUP",
                                       source_party="merchant", kind="MERCHANT_COMMITMENT",
                                       content={"v": 2})
        ev3 = self.svc.submit_evidence(self.cid, actor="agent", evidence_id="E-DUP",
                                       source_party="merchant", kind="MERCHANT_COMMITMENT",
                                       content={"v": 1})
        self.assertEqual(len(ev1), 1)
        self.assertEqual(ev2, [])
        self.assertEqual(ev3, [])
        case = self.store.load_case(self.cid)
        self.assertEqual(case.seq, after_first)
        self.assertEqual(case.evidences["E-DUP"].content, {"v": 1})
        self.assertGreater(after_first, before)

    def test_same_id_from_different_sources_both_kept(self):
        self.svc.submit_evidence(self.cid, actor="agent", evidence_id="SAME",
                                 source_party="merchant", kind="MERCHANT_COMMITMENT",
                                 content={"who": "merchant"})
        self.svc.submit_evidence(self.cid, actor="agent", evidence_id="SAME",
                                 source_party="platform", kind="BENEFIT_REDEEMED",
                                 content={"who": "platform"})
        case = self.store.load_case(self.cid)
        self.assertIn("merchant:SAME", case._dedup)
        self.assertIn("platform:SAME", case._dedup)

    def test_late_material_evidence_reopens_closed_case(self):
        self._finish_and_close()
        case = self.store.load_case(self.cid)
        self.assertEqual(case.status, "CLOSED")
        closed_seq = case.seq

        # 结案后第 3 天才到达的关键证据（监管协查取得的核销后台记录）
        t_late = clock.deadline_after(self.ctx["t0"], timedelta(days=3))
        self.svc.submit_evidence(self.cid, actor="agent", evidence_id="E-LATE-MATERIAL",
                                 source_party="platform", kind="BENEFIT_REDEEMED",
                                 content={"backend_log": "实际未核销"},
                                 material=True, at=t_late)
        case = self.store.load_case(self.cid)
        self.assertEqual(case.status, "REOPENED")
        self.assertEqual(case.reopen_count, 1)
        # 结案事件仍然保留在时间线上，没有被删除或改写
        types = [e["event_type"] for e in case.events]
        self.assertIn("CaseClosed", types)
        self.assertEqual(types[-1], "CaseReopened")
        self.assertGreater(case.seq, closed_seq)

        # 重审期间可以就新证据追加责任项；已到账补救不受影响
        settled_before = case.all_settled_cents()
        self.svc.add_item(self.cid, actor="agent", item_id="I3",
                          title="核销后台记录显示平台全责",
                          linked_evidence=["E-LATE-MATERIAL"])
        self.assertEqual(self.store.load_case(self.cid).all_settled_cents(),
                         settled_before)

    def test_late_immaterial_evidence_after_close_is_kept_without_reopen(self):
        self._finish_and_close()
        # 非关键迟到证据：照样收录以备追溯，但案件维持结案
        events = self.svc.submit_evidence(self.cid, actor="agent", evidence_id="E-NOISE",
                                          source_party="organizer", kind="EVENT_PUBLISHED",
                                          content={"note": "无关补充"}, material=False)
        self.assertEqual(len(events), 1)
        case = self.store.load_case(self.cid)
        self.assertEqual(case.status, "CLOSED")
        self.assertIn("E-NOISE", case.evidences)
        self.assertEqual(case.reopen_count, 0)

    def test_repeated_reopen_is_idempotent_per_evidence(self):
        self._finish_and_close()
        t = clock.deadline_after(self.ctx["t0"], timedelta(days=2))
        self.svc.submit_evidence(self.cid, actor="agent", evidence_id="E-M1",
                                 source_party="platform", kind="BENEFIT_REDEEMED",
                                 content={}, material=True, at=t)
        # 同一证据再次送达：既不重复加证据，也不重复重开
        self.svc.submit_evidence(self.cid, actor="agent", evidence_id="E-M1",
                                 source_party="platform", kind="BENEFIT_REDEEMED",
                                 content={}, material=True,
                                 at=clock.deadline_after(t, timedelta(hours=1)))
        case = self.store.load_case(self.cid)
        self.assertEqual(case.reopen_count, 1)


if __name__ == "__main__":
    unittest.main()
