"""完整生命周期与客服时间线测试。

覆盖：消费者选择终局性、结案必须等待全部必需动作、结案阻塞、
时间线能解释当前责任/下一步/金额守恒。
"""

from __future__ import annotations

import tempfile
import unittest

from src.jointcase import projections as P
from src.jointcase.domain import RuleViolation

from tests._helpers import build_two_item_case


class LifecycleTimelineTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ctx = build_two_item_case(self.tmp.name, case_id="C-LIFE")
        self.svc = self.ctx["svc"]
        self.store = self.ctx["store"]
        self.cid = "C-LIFE"

    def tearDown(self):
        self.tmp.cleanup()

    def test_close_waits_for_all_required_actions_and_choice(self):
        self.svc.consumer_choose(self.cid, actor="consumer", offer_id="O1",
                                 option_id="OPT-REFUND", accepted=True)
        self.svc.complete_action(self.cid, actor="merchant", action_id="A1")
        self.svc.complete_action(self.cid, actor="organizer", action_id="A2")
        # A3 未完成，不能结案
        with self.assertRaises(RuleViolation) as cm:
            self.svc.close_case(self.cid, actor="agent")
        self.assertIn("A3", str(cm.exception))
        self.svc.complete_action(self.cid, actor="platform", action_id="A3")
        self.svc.close_case(self.cid, actor="agent", summary="全部补救完成")
        self.assertEqual(self.store.load_case(self.cid).status, "CLOSED")

    def test_cannot_close_before_consumer_choice(self):
        for aid, actor in (("A1", "merchant"), ("A2", "organizer"), ("A3", "platform")):
            self.svc.complete_action(self.cid, actor=actor, action_id=aid)
        with self.assertRaises(RuleViolation):
            self.svc.close_case(self.cid, actor="agent")

    def test_consumer_choice_is_final(self):
        self.svc.consumer_choose(self.cid, actor="consumer", offer_id="O1",
                                 option_id="OPT-BENEFIT", accepted=True)
        with self.assertRaises(RuleViolation):
            self.svc.consumer_choose(self.cid, actor="consumer", offer_id="O1",
                                     option_id="OPT-REFUND", accepted=True)
        case = self.store.load_case(self.cid)
        self.assertEqual(case.current_offer.chosen_option_id, "OPT-BENEFIT")
        self.assertTrue(case.current_offer.accepted)

    def test_consumer_may_decline_then_new_offer_can_be_issued(self):
        # 消费者拒绝当前方案后，可以发出新的一轮选项
        self.svc.consumer_choose(self.cid, actor="consumer", offer_id="O1",
                                 option_id="", accepted=False)
        case = self.store.load_case(self.cid)
        self.assertFalse(case.current_offer.accepted)
        self.svc.offer_choices(
            self.cid, actor="organizer", offer_id="O2",
            valid_until=self.ctx["deadline"],
            options=[{"option_id": "OPT-2", "kind": "PARTIAL_REFUND",
                      "label": "追加后的退款方案", "amount_cents": 6000}],
        )
        self.assertEqual(self.store.load_case(self.cid).current_offer.offer_id, "O2")

    def test_timeline_explains_responsibility_next_step_and_money(self):
        case = self.store.load_case(self.cid)
        entries = P.timeline(case)
        # 每个责任项的认领与判罚都能在时间线上定位到责任方
        claim_entries = [e for e in entries if e["type"] == "LiabilityClaimed"]
        self.assertEqual({e["detail"]["claimed_by"] for e in claim_entries},
                         {"merchant", "platform"})

        summary = P.case_summary(case)
        # 当前责任：I1 商户、I2 平台，各自有待办动作
        owners = {s["owner"] for s in summary["next_steps"]}
        self.assertIn("merchant", owners)
        self.assertIn("platform", owners)
        self.assertIn("consumer", owners)  # 等待消费者选择
        # 金额守恒视图可解释
        money = summary["money"]
        self.assertEqual(money["decided_total_cents"], 6000)
        self.assertEqual(money["assigned_total_cents"], 4000)  # A3 是权益非资金
        self.assertEqual(money["settled_total_cents"], 0)
        self.assertTrue(money["balanced"])

        # 消费者选择、动作全部完成后，下一步只剩"可结案"
        self.svc.consumer_choose(self.cid, actor="consumer", offer_id="O1",
                                 option_id="OPT-REFUND", accepted=True)
        for aid in ("A1", "A2", "A3"):
            actor = self.store.load_case(self.cid).actions[aid].responsible_party
            self.svc.complete_action(self.cid, actor=actor, action_id=aid)
        summary = P.case_summary(self.store.load_case(self.cid))
        self.assertEqual(summary["next_steps"],
                         [{"step": "全部必需动作已完成，可结案", "owner": "organizer"}])
        self.svc.close_case(self.cid, actor="agent")
        summary = P.case_summary(self.store.load_case(self.cid))
        self.assertEqual(summary["next_steps"], [{"step": "案件已结案", "owner": None}])
        self.assertEqual(summary["money"]["settled_total_cents"], 4000)

    def test_double_refund_and_overcap_blocked_by_invariants(self):
        # A1 已是 3000 退款；再分派 3000 REFUND 直接超过判罚总额
        with self.assertRaises(RuleViolation):
            self.svc.assign_action(self.cid, actor="merchant", action_id="BAD",
                                   item_id="I1", kind="REFUND",
                                   responsible_party="merchant",
                                   deadline=self.ctx["deadline"], amount_cents=3000)
        # 负数/零金额资金动作非法（用仲裁角色，避免命令授权先拦截）
        with self.assertRaises(RuleViolation):
            self.svc.assign_action(self.cid, actor="arbiter", action_id="BAD2",
                                   item_id="I2", kind="REFUND",
                                   responsible_party="platform",
                                   deadline=self.ctx["deadline"], amount_cents=0)

    def test_decision_shares_must_sum_to_total(self):
        # 领域层直接验证：分摊之和 != 总额不可能发生（总额由分摊求和得出），
        # 但负分摊被拒绝
        with self.assertRaises(RuleViolation):
            self.svc.decide(self.cid, actor="platform", item_id="I2",
                            decision_id="DNEG", reason="负分摊",
                            shares_cents={"platform": -1})


if __name__ == "__main__":
    unittest.main()
