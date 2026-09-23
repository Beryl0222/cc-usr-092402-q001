"""场景四：人工改判。

- 改判必须显式标注被替代的当前决策版本（supersedes_event_seq）；
- 原决定、依据、分摊金额全部保留，可沿时间线追溯；
- 改判后补偿总额不得低于消费者已实际到账金额（已接受的补救不失效）；
- 改判产生的差额只能以补发（TOPUP_REFUND）形式新增动作，不能重复退款。
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import timedelta

from src.jointcase import clock
from src.jointcase.domain import RuleViolation
from src.jointcase import projections as P

from tests._helpers import build_two_item_case


class ReDecisionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ctx = build_two_item_case(self.tmp.name, case_id="C-REDECIDE")
        self.svc = self.ctx["svc"]
        self.store = self.ctx["store"]
        self.cid = "C-REDECIDE"

    def tearDown(self):
        self.tmp.cleanup()

    def test_redecision_keeps_original_version_and_basis(self):
        case = self.store.load_case(self.cid)
        first_seq = case.items["I1"].decisions[-1].event_seq

        # 仲裁复核后改判：总额 4000 -> 5000，主办方多承担 1000
        self.svc.decide(self.cid, actor="arbiter", item_id="I1",
                        decision_id="D1-2", reason="监管协查确认主办方安保告知不足",
                        basis_evidence=["E-MERCHANT", "E-TX"],
                        shares_cents={"merchant": 3000, "organizer": 2000},
                        supersedes_event_seq=first_seq)

        case = self.store.load_case(self.cid)
        decisions = case.items["I1"].decisions
        self.assertEqual(len(decisions), 2)
        old, new = decisions
        # 旧版本原样保留：事件序号、依据、理由、分摊
        self.assertEqual(old.event_seq, first_seq)
        self.assertEqual(old.decision_id, "D1-1")
        self.assertEqual(old.reason, "现场海报承诺凭证成立")
        self.assertEqual(old.basis_evidence, ["E-MERCHANT"])
        self.assertEqual(old.shares_cents, {"merchant": 3000, "organizer": 1000})
        self.assertEqual(new.supersedes_event_seq, first_seq)
        self.assertEqual(new.shares_cents, {"merchant": 3000, "organizer": 2000})

        # 时间线能解释"初判 -> 改判"
        entries = {e["seq"]: e for e in P.timeline(case)}
        self.assertEqual(entries[first_seq]["detail"]["revision"], "初判")
        self.assertEqual(entries[new.event_seq]["detail"]["revision"], "改判")

    def test_redecision_requires_current_version_pointer(self):
        case = self.store.load_case(self.cid)
        first_seq = case.items["I1"].decisions[-1].event_seq

        # 不标注被替代版本：拒绝
        with self.assertRaises(RuleViolation):
            self.svc.decide(self.cid, actor="arbiter", item_id="I1",
                            decision_id="D-BAD", reason="缺版本指针",
                            shares_cents={"merchant": 3000, "organizer": 2000})
        # 标注错误的历史序号：拒绝（防止并发下基于过期版本改判）
        with self.assertRaises(RuleViolation):
            self.svc.decide(self.cid, actor="arbiter", item_id="I1",
                            decision_id="D-BAD2", reason="错版本",
                            shares_cents={"merchant": 3000, "organizer": 2000},
                            supersedes_event_seq=first_seq - 1)

    def test_redecision_cannot_reduce_already_received_money(self):
        # 商户的 3000 退款已经到账
        self.svc.complete_action(self.cid, actor="merchant", action_id="A1",
                                 result_ref="paid-3000")
        case = self.store.load_case(self.cid)
        first_seq = case.items["I1"].decisions[-1].event_seq

        # 想把总额改判成 2000（低于消费者已到手的 3000）：拒绝
        with self.assertRaises(RuleViolation):
            self.svc.decide(self.cid, actor="arbiter", item_id="I1",
                            decision_id="D-LOW", reason="试图压低赔付",
                            shares_cents={"merchant": 2000},
                            supersedes_event_seq=first_seq)

    def test_redecision_topup_difference_without_double_refund(self):
        self.svc.complete_action(self.cid, actor="merchant", action_id="A1")
        case = self.store.load_case(self.cid)
        first_seq = case.items["I1"].decisions[-1].event_seq
        dl = self.ctx["deadline"]

        self.svc.decide(self.cid, actor="arbiter", item_id="I1",
                        decision_id="D1-2", reason="主办方责任加重",
                        basis_evidence=["E-MERCHANT", "E-TX"],
                        shares_cents={"merchant": 3000, "organizer": 2000},
                        supersedes_event_seq=first_seq)

        # 已分派 A1(3000)+A2(1000)=4000，只能补发差额 1000
        self.svc.assign_action(self.cid, actor="arbiter", action_id="A4",
                               item_id="I1", kind="TOPUP_REFUND",
                               responsible_party="organizer", deadline=dl,
                               amount_cents=1000)
        # 超过判罚总额的补发：拒绝
        with self.assertRaises(RuleViolation):
            self.svc.assign_action(self.cid, actor="arbiter", action_id="A5",
                                   item_id="I1", kind="TOPUP_REFUND",
                                   responsible_party="organizer", deadline=dl,
                                   amount_cents=2000)
        # 再来一笔 REFUND 也必须拒绝（A1 已是完成态退款）
        with self.assertRaises(RuleViolation):
            self.svc.assign_action(self.cid, actor="arbiter", action_id="A6",
                                   item_id="I1", kind="REFUND",
                                   responsible_party="organizer", deadline=dl,
                                   amount_cents=1000)

        case = self.store.load_case(self.cid)
        case.check_invariants()
        money = P.money_conservation(case)
        self.assertTrue(money["balanced"])
        self.assertEqual(money["decided_total_cents"], 7000)  # I1 5000 + I2 2000

    def test_concurrent_redecision_second_writer_retries_then_fails_on_version(self):
        """两个仲裁员同时改判：后者 CAS 重试后会因版本指针过期而被拒绝。"""
        import threading
        case = self.store.load_case(self.cid)
        first_seq = case.items["I1"].decisions[-1].event_seq
        errors: list[BaseException] = []

        def redecide(decision_id: str, barrier: threading.Barrier):
            local = self.svc.__class__(self.store.__class__(self.tmp.name))
            barrier.wait()
            try:
                local.decide(self.cid, actor="arbiter", item_id="I1",
                             decision_id=decision_id, reason="并发改判",
                             shares_cents={"merchant": 3000, "organizer": 2000},
                             supersedes_event_seq=first_seq)
            except BaseException as exc:  # noqa: BLE001 - 记录两个线程的结局
                errors.append(exc)

        barrier = threading.Barrier(2)
        threads = [threading.Thread(target=redecide, args=(f"D-C{i}", barrier))
                   for i in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        case = self.store.load_case(self.cid)
        self.assertEqual(len(case.items["I1"].decisions), 2)  # 只有一次改判成功
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RuleViolation)


if __name__ == "__main__":
    unittest.main()
