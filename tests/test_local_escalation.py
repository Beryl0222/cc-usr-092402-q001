"""场景三：局部超时升级。

- 某一方超时，只升级**对应责任项**：其他责任项的状态、时钟、补救不变；
- 升级不允许重复退款，也不能让消费者已经接受的补救失效；
- 升级可以携带新的监管截止时间，时钟重置；未真正超时不允许升级；
- 已完成的动作不能再被升级。
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import timedelta

from src.jointcase import clock
from src.jointcase.domain import RuleViolation
from src.jointcase.store import EventStore

from tests._helpers import build_two_item_case


class LocalEscalationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ctx = build_two_item_case(self.tmp.name, case_id="C-ESC")
        self.svc = self.ctx["svc"]
        self.store = self.ctx["store"]
        self.cid = "C-ESC"
        self.t0 = self.ctx["t0"]

    def tearDown(self):
        self.tmp.cleanup()

    def test_only_overdue_item_is_escalated(self):
        # 消费者选择后，商户先完成自己的退款；I2（平台）迟迟不动。
        self.svc.consumer_choose(self.cid, actor="consumer", offer_id="O1",
                                 option_id="OPT-BENEFIT", accepted=True)
        self.svc.complete_action(self.cid, actor="merchant", action_id="A1")
        self.svc.complete_action(self.cid, actor="organizer", action_id="A2")

        overdue_at = clock.deadline_after(self.t0, timedelta(hours=49))
        pending = self.svc.scan_pending(overdue_at)
        rec = next(r for r in pending if r.case_id == self.cid)
        self.assertEqual(rec.overdue_actions, ["A3"])

        escalated = self.svc.recover_and_escalate(overdue_at)
        self.assertEqual(escalated, {self.cid: ["A3"]})

        case = self.store.load_case(self.cid)
        self.assertEqual(case.items["I1"].status, "DECIDED")
        self.assertEqual(case.items["I1"].escalation_level, 0)
        self.assertEqual(case.items["I2"].status, "ESCALATED")
        self.assertEqual(case.items["I2"].escalation_level, 1)
        # I1 已完成的退款没有被回滚
        self.assertEqual(case.actions["A1"].status, "COMPLETED")
        self.assertEqual(case.actions["A2"].status, "COMPLETED")

    def test_escalation_resets_clock_and_does_not_double_escalate(self):
        overdue_at = clock.deadline_after(self.t0, timedelta(hours=49))
        new_deadline = clock.deadline_after(overdue_at, timedelta(hours=24))
        self.svc.escalate_overdue(self.cid, item_id="I2", action_id="A3",
                                  at=overdue_at, new_deadline=new_deadline,
                                  to_party="organizer")
        case = self.store.load_case(self.cid)
        action = case.actions["A3"]
        self.assertEqual(action.deadline, new_deadline)
        self.assertEqual(len(action.deadline_history), 2)  # 原截止 + 新截止

        # 新截止时间之前：A3 不应再次出现在超时清单（A1/A2 此刻本就超时，与 A3 无关）
        before_new = clock.deadline_after(overdue_at, timedelta(hours=12))
        rec = next(r for r in self.svc.scan_pending(before_new)
                   if r.case_id == self.cid)
        self.assertNotIn("A3", rec.overdue_actions)
        with self.assertRaises(RuleViolation):
            self.svc.escalate_overdue(self.cid, item_id="I2", action_id="A3",
                                      at=before_new)

        # 新时钟再次超时：第二次升级，级别变为 2
        overdue_again = clock.deadline_after(new_deadline, timedelta(hours=1))
        self.svc.recover_and_escalate(overdue_again)
        case = self.store.load_case(self.cid)
        self.assertEqual(case.items["I2"].escalation_level, 2)

    def test_cannot_escalate_before_deadline_or_after_completion(self):
        early = clock.deadline_after(self.t0, timedelta(hours=1))
        with self.assertRaises(RuleViolation):
            self.svc.escalate_overdue(self.cid, item_id="I1", action_id="A1", at=early)

        self.svc.complete_action(self.cid, actor="merchant", action_id="A1")
        late = clock.deadline_after(self.t0, timedelta(hours=99))
        with self.assertRaises(RuleViolation):
            self.svc.escalate_overdue(self.cid, item_id="I1", action_id="A1", at=late)

    def test_escalation_does_not_permit_double_refund(self):
        """超时升级后，补救仍受金额守恒约束：禁止第二笔退款。"""
        self.svc.complete_action(self.cid, actor="merchant", action_id="A1")
        overdue_at = clock.deadline_after(self.t0, timedelta(hours=49))
        self.svc.escalate_overdue(self.cid, item_id="I1", action_id="A2",
                                  at=overdue_at)
        # 有人想借"升级后重新处置"的名义再退一次 3000：必须拒绝
        new_dl = clock.deadline_after(overdue_at, timedelta(hours=12))
        with self.assertRaises(RuleViolation):
            self.svc.assign_action(self.cid, actor="organizer", action_id="A1-DUP",
                                   item_id="I1", kind="REFUND",
                                   responsible_party="merchant",
                                   deadline=new_dl, amount_cents=3000)
        case = self.store.load_case(self.cid)
        # 已接受的补救依然有效，金额守恒仍然成立
        case.check_invariants()
        self.assertEqual(case.settled_cents_for_item("I1"), 3000)

    def test_close_blocked_until_escalated_action_completed(self):
        self.svc.consumer_choose(self.cid, actor="consumer", offer_id="O1",
                                 option_id="OPT-REFUND", accepted=True)
        self.svc.complete_action(self.cid, actor="merchant", action_id="A1")
        self.svc.complete_action(self.cid, actor="organizer", action_id="A2")
        overdue_at = clock.deadline_after(self.t0, timedelta(hours=49))
        self.svc.escalate_overdue(self.cid, item_id="I2", action_id="A3",
                                  at=overdue_at)
        with self.assertRaises(RuleViolation):
            self.svc.close_case(self.cid, actor="agent")
        # 平台在新时限内完成替代权益后才能结案
        self.svc.complete_action(self.cid, actor="platform", action_id="A3")
        self.svc.close_case(self.cid, actor="agent")
        self.assertEqual(self.store.load_case(self.cid).status, "CLOSED")


if __name__ == "__main__":
    unittest.main()
