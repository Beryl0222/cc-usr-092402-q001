"""参与方最小可见视图测试。

未经授权的参与方只能看到自己负责的部分：自己认领的责任项、自己提交
的证据、分派给自己的动作、判罚中属于自己的分摊金额。
"""

from __future__ import annotations

import tempfile
import unittest

from src.jointcase import projections as P
from src.jointcase.service import AuthorizationError

from tests._helpers import build_two_item_case


class PartyVisibilityTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ctx = build_two_item_case(self.tmp.name, case_id="C-VIS")
        self.svc = self.ctx["svc"]
        self.store = self.ctx["store"]
        self.cid = "C-VIS"
        self.case = self.store.load_case(self.cid)

    def tearDown(self):
        self.tmp.cleanup()

    def test_merchant_sees_only_own_item_evidence_actions_share(self):
        view = P.party_view(self.case, "merchant")
        self.assertTrue(view["visible"])
        self.assertEqual([i["item_id"] for i in view["items"]], ["I1"])
        # 只看到自己提交的证据（E-MERCHANT），看不到平台的核销记录
        ev_ids = {e["evidence_id"] for e in view["evidences"]}
        self.assertEqual(ev_ids, {"E-MERCHANT"})
        # 只看到分派给自己的动作 A1，看不到 A2（主办方）和 A3（平台）
        self.assertEqual({a["action_id"] for a in view["actions"]}, {"A1"})
        # 判罚只暴露自己那一档金额
        decision = view["decisions"][0]
        self.assertEqual(decision["my_share_cents"], 3000)
        self.assertNotIn("shares_cents", decision)
        # 不应出现全案时间线/消费者信息
        self.assertNotIn("timeline", view)
        self.assertNotIn("consumer", view)

    def test_platform_cannot_see_merchant_item(self):
        view = P.party_view(self.case, "platform")
        self.assertEqual([i["item_id"] for i in view["items"]], ["I2"])
        self.assertEqual({a["action_id"] for a in view["actions"]}, {"A3"})

    def test_command_authorization_blocks_cross_party_mutation(self):
        # 平台不能给商户认领的责任项判罚
        with self.assertRaises(AuthorizationError):
            self.svc.decide(self.cid, actor="platform", item_id="I1",
                            decision_id="DX", reason="越权",
                            shares_cents={"platform": 1})
        # 平台不能替商户完成 A1
        with self.assertRaises(AuthorizationError):
            self.svc.complete_action(self.cid, actor="platform", action_id="A1")
        # 普通参与方不能私自结案
        with self.assertRaises(AuthorizationError):
            self.svc.close_case(self.cid, actor="merchant")

    def test_unknown_identity_gets_opaque_empty_view(self):
        view = P.party_view(self.case, "stranger-co")
        self.assertFalse(view["visible"])
        self.assertEqual(view["items"], [])
        self.assertEqual(view["actions"], [])

    def test_agent_and_arbiter_see_full_timeline(self):
        for viewer in ("agent", "arbiter", "regulator", "organizer"):
            view = P.party_view(self.case, viewer)
            self.assertIn("timeline", view)
            self.assertEqual(len(view["items"]), 2)
            self.assertIn("money", view)

    def test_consumer_view_hides_internal_shares(self):
        view = P.party_view(self.case, "consumer")
        self.assertIn("timeline", view)
        for item in view["items"]:
            if item["current_decision"]:
                self.assertNotIn("shares_cents", item["current_decision"])

    def test_finance_view_is_limited_to_settlement_data(self):
        view = P.party_view(self.case, "finance")
        self.assertNotIn("items", view)
        self.assertEqual(view["batches"], [])
        # 尚未完成的资金动作不会提前出现在财务可结算清单
        self.assertEqual(view["available_for_next_batch"], [])


if __name__ == "__main__":
    unittest.main()
