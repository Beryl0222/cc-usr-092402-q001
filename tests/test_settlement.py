"""财务结算批次测试：按已确认结果生成，不重不漏；改判差额进入后续批次。"""

from __future__ import annotations

import tempfile
import unittest
from datetime import timedelta

from src.jointcase import clock
from src.jointcase.domain import RuleViolation
from src.jointcase.settlement import SettlementRunner

from tests._helpers import build_two_item_case


class SettlementTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ctx = build_two_item_case(self.tmp.name, case_id="C-SETTLE")
        self.svc = self.ctx["svc"]
        self.store = self.ctx["store"]
        self.cid = "C-SETTLE"
        self.deadline = self.ctx["deadline"]

    def tearDown(self):
        self.tmp.cleanup()

    def _complete_fund_actions(self):
        self.svc.consumer_choose(self.cid, actor="consumer", offer_id="O1",
                                 option_id="OPT-REFUND", accepted=True)
        self.svc.complete_action(self.cid, actor="merchant", action_id="A1",
                                 result_ref="pay-3000")
        self.svc.complete_action(self.cid, actor="organizer", action_id="A2",
                                 result_ref="pay-1000")

    def test_batch_contains_only_confirmed_completed_fund_actions(self):
        # 动作未完成：运行不产生批次
        run = SettlementRunner(self.store).run(run_id="B0")
        self.assertEqual(run.line_count, 0)

        self._complete_fund_actions()
        run = SettlementRunner(self.store).run(run_id="B1")
        self.assertEqual(run.line_count, 2)
        self.assertEqual(run.total_cents, 4000)
        batch = run.batches[0]
        self.assertEqual(batch["currency"], "CNY")
        self.assertEqual(
            sorted(line["action_id"] for line in batch["lines"]), ["A1", "A2"]
        )

    def test_rerun_does_not_double_settle(self):
        self._complete_fund_actions()
        first = SettlementRunner(self.store).run(run_id="B1")
        self.assertEqual(first.total_cents, 4000)
        second = SettlementRunner(self.store).run(run_id="B2")
        self.assertEqual(second.line_count, 0)
        self.assertEqual(second.total_cents, 0)

        case = self.store.load_case(self.cid)
        # 事件层只有一个批次事件；同一动作只在一个批次中
        self.assertEqual(len(case.settlement_batches), 1)
        self.assertEqual(case.batched_action_ids, {"A1", "A2"})

    def test_redecision_topup_flows_into_next_batch(self):
        self._complete_fund_actions()
        SettlementRunner(self.store).run(run_id="B1")

        case = self.store.load_case(self.cid)
        first_seq = case.current_decision("I1").event_seq
        self.svc.decide(self.cid, actor="arbiter", item_id="I1", decision_id="D1-2",
                        reason="主办方责任加重", basis_evidence=["E-MERCHANT"],
                        shares_cents={"merchant": 3000, "organizer": 2000},
                        supersedes_event_seq=first_seq)
        self.svc.assign_action(self.cid, actor="arbiter", action_id="A4",
                               item_id="I1", kind="TOPUP_REFUND",
                               responsible_party="organizer", deadline=self.deadline,
                               amount_cents=1000)
        # 补发尚未完成：不会进批次
        self.assertEqual(SettlementRunner(self.store).run(run_id="B2").line_count, 0)
        self.svc.complete_action(self.cid, actor="organizer", action_id="A4",
                                 result_ref="pay-topup-1000")
        run3 = SettlementRunner(self.store).run(run_id="B3")
        self.assertEqual(run3.line_count, 1)
        self.assertEqual(run3.total_cents, 1000)
        self.assertEqual(run3.batches[0]["lines"][0]["action_id"], "A4")

        report = SettlementRunner(self.store).report()
        case_report = next(c for c in report["cases"] if c["case_id"] == self.cid)
        self.assertTrue(case_report["no_duplicate"])
        self.assertTrue(case_report["no_missing"])
        self.assertEqual(case_report["unbatched_action_ids"], [])
        self.assertTrue(report["all_balanced"])
        # 消费者累计到手 5000，批次累计 4000 + 1000
        self.assertEqual(report["settled_total_cents"], 5000)
        self.assertEqual(report["batched_total_cents"], 5000)

    def test_confirm_batch_rechecks_current_state(self):
        self._complete_fund_actions()
        SettlementRunner(self.store).run(run_id="B1")
        self.svc.confirm_settlement_batch(
            self.cid, actor="finance", batch_id="B1-C-SETTLE"
        )
        # 重复确认拒绝
        with self.assertRaises(RuleViolation):
            self.svc.confirm_settlement_batch(
                self.cid, actor="finance", batch_id="B1-C-SETTLE"
            )
        case = self.store.load_case(self.cid)
        self.assertEqual(case.confirmed_batches, {"B1-C-SETTLE"})

    def test_manual_batch_rejects_unfinished_or_duplicate_actions(self):
        self._complete_fund_actions()
        SettlementRunner(self.store).run(run_id="B1")
        # A1 已在 B1：手工再纳入必须被拒绝
        with self.assertRaises(RuleViolation):
            self.svc.create_settlement_batch(self.cid, actor="finance",
                                             batch_id="BX", action_ids=["A1"])

    def test_multi_case_run_covers_every_case_once(self):
        """第二个案件也被同一轮结算覆盖（不漏案件）。"""
        self._complete_fund_actions()
        other = build_two_item_case(self.tmp.name, case_id="C-SETTLE-2",
                                    t0=self.ctx["t0"])
        other_svc = other["svc"]
        other_svc.consumer_choose("C-SETTLE-2", actor="consumer", offer_id="O1",
                                  option_id="OPT-REFUND", accepted=True)
        other_svc.complete_action("C-SETTLE-2", actor="merchant", action_id="A1")
        other_svc.complete_action("C-SETTLE-2", actor="organizer", action_id="A2")

        run = SettlementRunner(self.store).run(run_id="BM")
        self.assertEqual(sorted(run.per_case), ["C-SETTLE", "C-SETTLE-2"])
        self.assertEqual(run.line_count, 4)
        report = SettlementRunner(self.store).report()
        self.assertTrue(report["all_balanced"])


if __name__ == "__main__":
    unittest.main()
