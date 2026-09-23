"""场景五：故障恢复。

服务重启后没有独立的待办数据库——扫描并重放全部案件事件日志即可
恢复待办；对确已超过监管时限的责任项执行局部升级，结果与持续在线时
一致。崩溃发生在事件落盘前后的两种情况都必须保持一致状态。
"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

from src.jointcase import clock
from src.jointcase.domain import RuleViolation
from src.jointcase.store import EventStore
from src.jointcase.service import CaseService

from tests._helpers import build_two_item_case


class RecoveryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ctx = build_two_item_case(self.tmp.name, case_id="C-RECOVER")
        self.svc = self.ctx["svc"]
        self.store = self.ctx["store"]
        self.cid = "C-RECOVER"
        self.t0 = self.ctx["t0"]

    def tearDown(self):
        self.tmp.cleanup()

    def test_fresh_service_restores_state_from_log(self):
        # 模拟全新进程：新建 service 实例、不持有任何内存状态
        restarted = CaseService(EventStore(self.tmp.name))
        case = restarted.store.load_case(self.cid)
        self.assertEqual(set(case.items), {"I1", "I2"})
        self.assertEqual(case.items["I1"].claimed_by, "merchant")
        self.assertEqual(case.current_decision("I2").shares_cents, {"platform": 2000})
        self.assertEqual(set(case.actions), {"A1", "A2", "A3"})
        self.assertEqual(case.status, "OPEN")

    def test_scan_pending_after_restart_lists_overdue_and_waiting_choice(self):
        restarted = CaseService(EventStore(self.tmp.name))
        at = clock.deadline_after(self.t0, timedelta(hours=49))
        report = restarted.scan_pending(at)
        rec = next(r for r in report if r.case_id == self.cid)
        self.assertEqual(sorted(rec.pending_actions), ["A1", "A2", "A3"])
        self.assertEqual(sorted(rec.overdue_actions), ["A1", "A2", "A3"])
        self.assertTrue(rec.awaiting_consumer_choice)

        # 已结案案件不出现在待办扫描中
        self.svc.consumer_choose(self.cid, actor="consumer", offer_id="O1",
                                 option_id="OPT-REFUND", accepted=True)
        for aid in ("A1", "A2", "A3"):
            self.svc.complete_action(self.cid, actor="agent", action_id=aid)
        self.svc.close_case(self.cid, actor="agent")
        self.assertEqual(
            [r for r in CaseService(EventStore(self.tmp.name)).scan_pending(at)
             if r.case_id == self.cid],
            [],
        )

    def test_restart_recovery_escalates_only_overdue_items(self):
        # 重启前 A1/A2 已完成，只有 A3 超时
        self.svc.complete_action(self.cid, actor="merchant", action_id="A1")
        self.svc.complete_action(self.cid, actor="organizer", action_id="A2")
        at = clock.deadline_after(self.t0, timedelta(hours=72))

        restarted = CaseService(EventStore(self.tmp.name))
        escalated = restarted.recover_and_escalate(at)
        self.assertEqual(escalated, {self.cid: ["A3"]})

        # 再次重启并再次恢复：不重复升级（时钟未重置则该动作已标 escalated）
        second = CaseService(EventStore(self.tmp.name))
        again = second.recover_and_escalate(at)
        self.assertEqual(again, {})

        case = EventStore(self.tmp.name).load_case(self.cid)
        self.assertEqual(case.items["I2"].escalation_level, 1)
        self.assertEqual(case.actions["A1"].status, "COMPLETED")

    def test_recovery_idempotent_with_online_processing(self):
        """持续在线处理与重启恢复交替执行，升级结果仍只有一次。"""
        at = clock.deadline_after(self.t0, timedelta(hours=49))
        # 在线调度器先升级一次 A1
        self.svc.escalate_overdue(self.cid, item_id="I1", action_id="A1", at=at)
        # 紧接着"重启恢复"扫描：A1 已升级，不应重复；A2、A3 照常升级
        escalated = CaseService(EventStore(self.tmp.name)).recover_and_escalate(at)
        self.assertEqual(escalated, {self.cid: ["A2", "A3"]})
        case = self.store.load_case(self.cid)
        # A1 与 A2 都挂在 I1：I1 因两次超时升级为 2，I2 为 1
        self.assertEqual(case.items["I1"].escalation_level, 2)
        self.assertEqual(case.items["I2"].escalation_level, 1)

    def test_corrupt_or_partial_last_line_is_detected(self):
        """崩溃在写一半时：截断的最后一行被明确报为日志损坏，不静默吞掉。"""
        log = Path(self.tmp.name) / f"{self.cid}.events.jsonl"
        raw = log.read_bytes()
        log.write_bytes(raw[: len(raw) - 3])  # 截断最后一条 JSON
        with self.assertRaises(Exception) as cm:
            EventStore(self.tmp.name).load_case(self.cid)
        self.assertIn("损坏", str(cm.exception))

    def test_replay_of_log_file_matches_serialized_events(self):
        """落盘的事件信封本身可被外部工具解析（监管审计可读 JSONL）。"""
        log = Path(self.tmp.name) / f"{self.cid}.events.jsonl"
        lines = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
        self.assertEqual([e["seq"] for e in lines], list(range(len(lines))))
        for envelope in lines:
            self.assertEqual(envelope["case_id"], self.cid)
            self.assertIn(envelope["event_type"], {
                "CaseOpened", "EvidenceSubmitted", "LiabilityItemAdded",
                "LiabilityClaimed", "DecisionRecorded", "ActionAssigned",
                "ConsumerOffered",
            })

    def test_close_then_reopen_then_reclose_stays_consistent(self):
        """结案 -> 迟到关键证据重审 -> 补动作 -> 再结案，全程守恒可恢复。"""
        self.svc.consumer_choose(self.cid, actor="consumer", offer_id="O1",
                                 option_id="OPT-REFUND", accepted=True)
        for aid, actor in (("A1", "merchant"), ("A2", "organizer"), ("A3", "platform")):
            self.svc.complete_action(self.cid, actor=actor, action_id=aid)
        self.svc.close_case(self.cid, actor="agent")

        t_late = clock.deadline_after(self.t0, timedelta(days=4))
        self.svc.submit_evidence(self.cid, actor="agent", evidence_id="E-NEW",
                                 source_party="platform", kind="BENEFIT_REDEEMED",
                                 content={"log": True}, material=True, at=t_late)
        self.svc.add_item(self.cid, actor="agent", item_id="I3",
                          title="新增责任", linked_evidence=["E-NEW"])
        self.svc.claim_item(self.cid, party_id="platform", item_id="I3")
        self.svc.decide(self.cid, actor="platform", item_id="I3", decision_id="D3-1",
                        reason="新证据确认", shares_cents={"platform": 500},
                        basis_evidence=["E-NEW"], at=t_late)
        self.svc.assign_action(self.cid, actor="platform", action_id="A7",
                               item_id="I3", kind="CLARIFICATION",
                               responsible_party="platform",
                               deadline=clock.deadline_after(t_late, timedelta(hours=24)))
        # 新责任项动作未完成，不能再结案
        with self.assertRaises(RuleViolation):
            self.svc.close_case(self.cid, actor="agent")
        self.svc.complete_action(self.cid, actor="platform", action_id="A7", at=t_late)
        self.svc.close_case(self.cid, actor="agent", at=t_late)

        # 重启后核对：两个结案事件、一次重开、资金守恒
        case = EventStore(self.tmp.name).load_case(self.cid)
        self.assertEqual(case.status, "CLOSED")
        self.assertEqual(
            [e["event_type"] for e in case.events].count("CaseClosed"), 2
        )
        case.check_invariants()


if __name__ == "__main__":
    unittest.main()
