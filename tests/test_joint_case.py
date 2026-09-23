"""联合处置案件的自动化测试。

覆盖：并发认领、迟到/重复证据、局部升级、人工改判留痕、
跨时区截止时间、金额守恒与结算不重不漏、按方可见性、
个人材料撤回、以及基于 JSONL 重放的故障恢复。
"""

from __future__ import annotations

import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from src.joint_case import (
    ACTION_OTHER,
    ACTION_REFUND,
    ACTION_REISSUE_BENEFIT,
    BATCH_CONFIRMED,
    DECISION_ACCEPTED,
    DECISION_FULFILLED,
    DECISION_PROPOSED,
    DECISION_SUPERSEDED,
    DEFAULT_GRACE,
    EVIDENCE_TRANSACTION_PROOF,
    EVIDENCE_WITHDRAWN,
    ITEM_CLAIMED,
    ITEM_COMPLETED,
    ITEM_OPEN,
    CaseError,
    FixedClock,
    FileEventStore,
    InMemoryEventStore,
    JointCase,
)

T0 = datetime(2026, 9, 23, 2, 0, tzinfo=timezone.utc)  # 10:00 北京
OPERATOR = "cs-platform"
MERCHANT = "venue-merchant"
BENEFIT = "benefit-platform"
CONSUMER = "consumer-7"
PARTICIPANTS = {
    OPERATOR: "operator",
    MERCHANT: "party",
    BENEFIT: "party",
}


def make_case(clock=None, store=None, grace=DEFAULT_GRACE):
    return JointCase.open(
        "case-1",
        complaint_ref="CMP-2026-0901",
        consumer_id=CONSUMER,
        participants=PARTICIPANTS,
        store=store or InMemoryEventStore(),
        clock=clock or FixedClock(T0),
    )


def open_refund_item(case, item_id, title, party, action=ACTION_REFUND,
                     evidence_id=None, due_at=None, required=True):
    """创建责任项并挂上交易凭证。"""
    case.add_item(item_id, title, party, action, due_at=due_at, required=required)
    case.submit_evidence(
        evidence_id or f"ev-{item_id}",
        EVIDENCE_TRANSACTION_PROOF,
        f"hash-{evidence_id or item_id}",
        CONSUMER, T0 - timedelta(days=2), item_id=item_id,
    )


def complete_refund(case, item_id, party, decision_id, amount, evidence_id=None):
    """认领 → 决定（附凭证依据）→ 消费者接受 → 履行。"""
    evidence_id = evidence_id or f"ev-{item_id}"
    case.claim_item(item_id, party)
    case.propose_decision(
        item_id, decision_id, ACTION_REFUND, amount, party, basis=[evidence_id],
    )
    case.accept_decision(item_id, decision_id, CONSUMER)
    case.fulfill_remedy(item_id, f"pay-{decision_id}", party)


def refund_flow(case, item_id, party, decision_id, amount, evidence_id=None,
                action=ACTION_REFUND, title="部分退款"):
    open_refund_item(case, item_id, title, party, action=action,
                     evidence_id=evidence_id, due_at=T0 + timedelta(hours=24))
    complete_refund(case, item_id, party, decision_id, amount, evidence_id=evidence_id)


class EvidenceTest(unittest.TestCase):
    def test_duplicate_and_late_evidence_is_idempotent(self):
        case = make_case()
        case.add_item("i1", "退款", MERCHANT, ACTION_REFUND, due_at=T0 + timedelta(hours=24))
        first = case.submit_evidence(
            "ev-1", EVIDENCE_TRANSACTION_PROOF, "hash-1", CONSUMER,
            T0 - timedelta(days=3), item_id="i1",
        )
        # 相同证据在责任认定后才迟到送达 / 网络重试重复送达：返回原事件，不新增时间线。
        seq_before = case.state["sequence"]
        duplicate = case.submit_evidence(
            "ev-1", EVIDENCE_TRANSACTION_PROOF, "hash-1", CONSUMER,
            T0 - timedelta(days=3), item_id="i1", now=T0 + timedelta(hours=10),
        )
        self.assertEqual(first, duplicate)
        self.assertEqual(case.state["sequence"], seq_before)
        self.assertEqual(len(case.state["evidence"]), 1)
        self.assertEqual(case.state["evidence"]["ev-1"]["received_at"], first["at"])

    def test_withdrawn_personal_material_leaves_tombstone(self):
        case = make_case()
        case.add_item("i-mer", "退款", MERCHANT, ACTION_REFUND)
        case.submit_evidence(
            "ev-pii", "OTHER", "hash-pii", CONSUMER, T0 - timedelta(days=1),
            item_id="i-mer",
        )
        case.withdraw_evidence("ev-pii", CONSUMER)
        record = case.state["evidence"]["ev-pii"]
        self.assertEqual(record["status"], EVIDENCE_WITHDRAWN)
        self.assertIsNotNone(record["withdrawn_at"])
        # 其他参与方视图中只剩墓碑，看不到内容哈希。
        view = case.view_for(MERCHANT)
        entry = next(e for e in view["evidence"] if e["evidence_id"] == "ev-pii")
        self.assertIsNone(entry["content_hash"])
        self.assertEqual(entry["status"], EVIDENCE_WITHDRAWN)
        # 提交者本人仍可核对自己撤回的材料指纹。
        own = case.view_for(CONSUMER)
        own_entry = next(e for e in own["evidence"] if e["evidence_id"] == "ev-pii")
        self.assertEqual(own_entry["content_hash"], "hash-pii")

    def test_duplicate_redemption_is_idempotent(self):
        case = make_case()
        first = case.record_redemption(
            "rd-1", "benefit-SK-001", BENEFIT, "hash-rd", T0 - timedelta(days=1),
        )
        before = case.state["sequence"]
        duplicate = case.record_redemption(
            "rd-1", "benefit-SK-001", BENEFIT, "hash-rd", T0 - timedelta(days=1),
            now=T0 + timedelta(hours=5),
        )
        self.assertEqual(first, duplicate)
        self.assertEqual(case.state["sequence"], before)

    def test_duplicate_command_id_is_rejected(self):
        case = make_case()
        case.add_item("i1", "退款", MERCHANT, ACTION_REFUND, command_id="cmd-1")
        with self.assertRaises(CaseError) as raised:
            case.add_item("i2", "退款", MERCHANT, ACTION_REFUND, command_id="cmd-1")
        self.assertEqual(raised.exception.code, "DUPLICATE_COMMAND")
        self.assertNotIn("i2", case.state["items"])


class ConcurrencyClaimTest(unittest.TestCase):
    def test_concurrent_claim_only_one_succeeds(self):
        # 两个被指派到同一责任项的参与方……此处验证同一方重复并发认领也只能成功一次。
        case = make_case()
        case.add_item("i1", "退款", MERCHANT, ACTION_REFUND, due_at=T0 + timedelta(hours=24))

        outcomes = []

        def claim():
            try:
                case.claim_item("i1", MERCHANT)
                outcomes.append("ok")
            except CaseError as exc:
                outcomes.append(exc.code)

        threads = 16
        with ThreadPoolExecutor(max_workers=threads) as pool:
            list(pool.map(lambda _: claim(), range(threads)))

        self.assertEqual(outcomes.count("ok"), 1)
        self.assertEqual(outcomes.count("ITEM_NOT_CLAIMABLE"), threads - 1)
        self.assertEqual(case.state["items"]["i1"]["status"], ITEM_CLAIMED)
        self.assertEqual(case.state["items"]["i1"]["claimed_by"], MERCHANT)

    def test_non_responsible_party_cannot_claim(self):
        case = make_case()
        case.add_item("i1", "退款", MERCHANT, ACTION_REFUND, due_at=T0 + timedelta(hours=24))
        with self.assertRaises(CaseError) as raised:
            case.claim_item("i1", BENEFIT)
        self.assertEqual(raised.exception.code, "NOT_RESPONSIBLE")
        self.assertEqual(case.state["items"]["i1"]["status"], ITEM_OPEN)

    def test_unauthorized_party_cannot_open_view(self):
        case = make_case()
        with self.assertRaises(CaseError):
            case.view_for("outsider")


class EscalationClockTest(unittest.TestCase):
    def test_timezone_aware_due_and_levels(self):
        # 截止时间用东京时区（UTC+9）表达：2026-09-24 10:00 JST == 01:00 UTC。
        due_tokyo = "2026-09-24T10:00:00+09:00"
        case = make_case()
        case.add_item("i1", "退款", MERCHANT, ACTION_REFUND, due_at=due_tokyo)
        self.assertEqual(case.state["items"]["i1"]["due_at"], "2026-09-24T01:00:00+00:00")

        # 01:00 UTC 前一刻不升级。
        self.assertEqual(case.sweep(now=datetime(2026, 9, 24, 0, 59, tzinfo=timezone.utc)), [])
        # 到点 L1。
        fired = case.sweep(now=datetime(2026, 9, 24, 1, 0, tzinfo=timezone.utc))
        self.assertEqual(len(fired), 1)
        self.assertEqual(fired[0]["level"], 1)
        # 重复扫描（含服务重启后重放再扫）幂等，不产生第二次 L1。
        self.assertEqual(case.sweep(now=datetime(2026, 9, 24, 2, 0, tzinfo=timezone.utc)), [])
        # 超过 24h 宽限 -> L2。
        fired = case.sweep(now=datetime(2026, 9, 25, 1, 0, tzinfo=timezone.utc))
        self.assertEqual(len(fired), 1)
        self.assertEqual(fired[0]["level"], 2)
        self.assertEqual(len(case.state["items"]["i1"]["escalations"]), 2)
        # 此后再扫不升级。
        self.assertEqual(case.sweep(now=datetime(2026, 9, 26, tzinfo=timezone.utc)), [])

    def test_local_escalation_does_not_refund_or_touch_other_items(self):
        case = make_case()
        case.add_item("i-merchant", "商户退款", MERCHANT, ACTION_REFUND,
                      due_at=T0 + timedelta(hours=1))
        case.add_item("i-benefit", "权益补发", BENEFIT, ACTION_REISSUE_BENEFIT,
                      due_at=T0 + timedelta(hours=72))
        case.claim_item("i-benefit", BENEFIT)

        fired = case.sweep(now=T0 + timedelta(hours=2))
        self.assertEqual([e["item_id"] for e in fired], ["i-merchant"])
        merchant_item = case.state["items"]["i-merchant"]
        benefit_item = case.state["items"]["i-benefit"]
        self.assertEqual(len(merchant_item["escalations"]), 1)
        self.assertEqual(merchant_item["status"], ITEM_OPEN)  # 升级不自动完成/退款
        self.assertEqual(benefit_item["status"], ITEM_CLAIMED)
        self.assertEqual(benefit_item["escalations"], [])

    def test_due_items_recovered_after_restart_query(self):
        store = InMemoryEventStore()
        case = make_case(store=store)
        case.add_item("i1", "退款", MERCHANT, ACTION_REFUND, due_at=T0 + timedelta(hours=1))
        restored = JointCase.rebuild(store, clock=FixedClock(T0 + timedelta(hours=2)))
        due = restored.due_items()
        self.assertEqual([i["item_id"] for i in due], ["i1"])
        restored.sweep()
        self.assertEqual(len(restored.state["items"]["i1"]["escalations"]), 1)


class OverrideTest(unittest.TestCase):
    def _accepted_case(self):
        """停在“消费者已接受、尚未履行”的案件，供改判测试使用。"""
        case = make_case()
        case.add_item("i1", "部分退款", MERCHANT, ACTION_REFUND)
        case.submit_evidence(
            "ev-proof", EVIDENCE_TRANSACTION_PROOF, "hash-ev-proof",
            CONSUMER, T0, item_id="i1",
        )
        case.claim_item("i1", MERCHANT)
        case.propose_decision(
            "i1", "d1", ACTION_REFUND, "80.00", MERCHANT, basis=["ev-proof"],
        )
        case.accept_decision("i1", "d1", CONSUMER)
        return case

    def test_override_keeps_original_decision_and_basis(self):
        case = self._accepted_case()
        old = case.state["decisions"]["d1"]
        self.assertEqual(old["status"], DECISION_ACCEPTED)

        case.propose_override(
            "i1", "d2", ACTION_REFUND, "120.00",
            reason="迟到的交易凭证显示原承诺金额更高", by=OPERATOR,
            basis=["ev-proof"],
        )
        # 改判待确认期间，原决定仍生效；不能履行旧决定。
        self.assertEqual(case.state["decisions"]["d1"]["status"], DECISION_ACCEPTED)
        self.assertEqual(case.state["items"]["i1"]["current_decision_id"], "d1")
        with self.assertRaises(CaseError) as raised:
            case.fulfill_remedy("i1", "pay-d1", MERCHANT)
        self.assertEqual(raised.exception.code, "OVERRIDE_PENDING")
        # 消费者也不能在改判期间回头接受旧决定。
        with self.assertRaises(CaseError):
            case.accept_decision("i1", "d1", CONSUMER)

        case.accept_decision("i1", "d2", CONSUMER)
        self.assertEqual(case.state["decisions"]["d1"]["status"], DECISION_SUPERSEDED)
        self.assertEqual(case.state["decisions"]["d1"]["superseded_reason"],
                         "迟到的交易凭证显示原承诺金额更高")
        self.assertEqual(case.state["decisions"]["d2"]["status"], DECISION_ACCEPTED)
        self.assertEqual(case.state["items"]["i1"]["current_decision_id"], "d2")

    def test_withdraw_override_restores_original(self):
        case = self._accepted_case()
        case.propose_override("i1", "d2", ACTION_REFUND, "120.00",
                              reason="试算", by=OPERATOR)
        case.withdraw_override("i1", OPERATOR)
        self.assertEqual(case.state["decisions"]["d1"]["status"], DECISION_ACCEPTED)
        self.assertEqual(case.state["decisions"]["d2"]["status"], DECISION_SUPERSEDED)
        self.assertIsNone(case.state["items"]["i1"]["pending_override"])
        case.fulfill_remedy("i1", "pay-d1", MERCHANT)
        self.assertEqual(case.state["items"]["i1"]["status"], ITEM_COMPLETED)

    def test_fulfilled_remedy_cannot_be_overridden_or_refunded_again(self):
        case = self._accepted_case()
        case.fulfill_remedy("i1", "pay-d1", MERCHANT)
        with self.assertRaises(CaseError) as raised:
            case.propose_override("i1", "d2", ACTION_REFUND, "200.00",
                                  reason="重复退款尝试", by=OPERATOR)
        self.assertEqual(raised.exception.code, "REMEDY_LOCKED")
        with self.assertRaises(CaseError):
            case.fulfill_remedy("i1", "pay-again", MERCHANT)

    def test_cannot_propose_second_decision_without_override(self):
        case = make_case()
        case.add_item("i1", "退款", MERCHANT, ACTION_REFUND)
        case.claim_item("i1", MERCHANT)
        case.propose_decision("i1", "d1", ACTION_REFUND, "80.00", MERCHANT)
        with self.assertRaises(CaseError) as raised:
            case.propose_decision("i1", "d1b", ACTION_REFUND, "90.00", MERCHANT)
        self.assertEqual(raised.exception.code, "DECISION_PENDING")
        case.accept_decision("i1", "d1", CONSUMER)
        with self.assertRaises(CaseError) as raised:
            case.propose_decision("i1", "d1c", ACTION_REFUND, "90.00", MERCHANT)
        self.assertEqual(raised.exception.code, "DECISION_EXISTS")


class CloseAndSettlementTest(unittest.TestCase):
    def test_case_cannot_close_until_all_required_items_done(self):
        case = make_case()
        case.add_item("i1", "商户退款", MERCHANT, ACTION_REFUND,
                      due_at=T0 + timedelta(hours=24))
        case.add_item("i2", "权益补发", BENEFIT, ACTION_REISSUE_BENEFIT,
                      due_at=T0 + timedelta(hours=24))
        case.submit_evidence("ev-1", EVIDENCE_TRANSACTION_PROOF, "hash-ev-1",
                             CONSUMER, T0, item_id="i1")
        complete_refund(case, "i1", MERCHANT, "d1", "80.00", evidence_id="ev-1")
        with self.assertRaises(CaseError) as raised:
            case.close_case()
        self.assertEqual(raised.exception.code, "REQUIRED_ITEMS_OPEN")
        self.assertIn("i2", str(raised.exception))

        case.submit_evidence("ev-2", EVIDENCE_TRANSACTION_PROOF, "hash-ev-2",
                             CONSUMER, T0, item_id="i2")
        complete_refund(case, "i2", BENEFIT, "d2", "0", evidence_id="ev-2")
        case.close_case()
        self.assertEqual(case.state["case"]["status"], "CLOSED")

    def test_optional_item_cancelled_does_not_block_close(self):
        case = make_case()
        case.add_item("i1", "说明函", MERCHANT, ACTION_OTHER, required=False)
        case.claim_item("i1", MERCHANT)
        case.cancel_item("i1", "无需补充说明", OPERATOR)
        case.close_case()
        self.assertEqual(case.state["case"]["status"], "CLOSED")

    def test_required_item_cannot_be_cancelled(self):
        case = make_case()
        case.add_item("i1", "退款", MERCHANT, ACTION_REFUND)
        with self.assertRaises(CaseError) as raised:
            case.cancel_item("i1", "x", OPERATOR)
        self.assertEqual(raised.exception.code, "ITEM_REQUIRED")

    def test_settlement_batches_are_complete_and_non_duplicative(self):
        case = make_case()
        case.add_item("i1", "商户退款", MERCHANT, ACTION_REFUND)
        case.add_item("i2", "权益平台补偿", BENEFIT, ACTION_REFUND)
        case.add_item("i3", "说明函", MERCHANT, ACTION_OTHER)
        case.submit_evidence("ev-1", EVIDENCE_TRANSACTION_PROOF, "hash-ev-1",
                             CONSUMER, T0, item_id="i1")
        case.submit_evidence("ev-2", EVIDENCE_TRANSACTION_PROOF, "hash-ev-2",
                             CONSUMER, T0, item_id="i2")
        complete_refund(case, "i1", MERCHANT, "d1", "80.00", evidence_id="ev-1")
        complete_refund(case, "i2", BENEFIT, "d2", "30.50", evidence_id="ev-2")
        case.claim_item("i3", MERCHANT)
        case.complete_action("i3", "已出具说明", MERCHANT)

        audit = case.settlement_audit()
        self.assertTrue(audit["ok"])
        self.assertEqual(audit["expected"], {"CNY": Decimal("110.50")})
        self.assertEqual(audit["unbatched"], {"CNY": Decimal("110.50")})

        # 两笔补救都已完成：第一批一次纳入全部待结算条目。
        batch1 = case.create_settlement_batch("B-1", OPERATOR)
        item_ids = {e["item_id"] for e in batch1["entries"]}
        self.assertEqual(item_ids, {"i1", "i2"})
        case.confirm_batch("B-1", OPERATOR)
        self.assertEqual(case.state["batches"]["B-1"]["status"], BATCH_CONFIRMED)

        # 没有新完成项时不允许造空批次；重复确认被拒绝。
        with self.assertRaises(CaseError) as raised:
            case.create_settlement_batch("B-2", OPERATOR)
        self.assertEqual(raised.exception.code, "NOTHING_TO_SETTLE")
        with self.assertRaises(CaseError):
            case.confirm_batch("B-1", OPERATOR)

        # 再完成一笔补救后，下一批只纳入新项，老项不会重复出现。
        open_refund_item(case, "i4", "追加补偿", BENEFIT, required=False)
        complete_refund(case, "i4", BENEFIT, "d4", "19.50")
        batch2 = case.create_settlement_batch("B-2", OPERATOR)
        self.assertEqual([e["item_id"] for e in batch2["entries"]], ["i4"])

        audit = case.settlement_audit()
        self.assertTrue(audit["ok"])
        self.assertEqual(audit["expected"], {"CNY": Decimal("130.00")})
        self.assertEqual(audit["batched"], {"CNY": Decimal("130.00")})
        self.assertEqual(audit["unbatched"], {})

    def test_money_conservation_after_override_settles_final_amount_once(self):
        case = make_case()
        case.add_item("i1", "退款", MERCHANT, ACTION_REFUND)
        case.claim_item("i1", MERCHANT)
        case.submit_evidence("ev-1", EVIDENCE_TRANSACTION_PROOF, "h1", CONSUMER, T0)
        case.propose_decision("i1", "d1", ACTION_REFUND, "80.00", MERCHANT, basis=["ev-1"])
        case.accept_decision("i1", "d1", CONSUMER)
        case.propose_override("i1", "d2", ACTION_REFUND, "120.00",
                              reason="凭证复核", by=OPERATOR, basis=["ev-1"])
        case.accept_decision("i1", "d2", CONSUMER)
        case.fulfill_remedy("i1", "pay-d2", MERCHANT)
        batch = case.create_settlement_batch("B-1", OPERATOR)
        self.assertEqual(len(batch["entries"]), 1)
        self.assertEqual(batch["entries"][0]["amount"], "120.00")
        self.assertEqual(batch["entries"][0]["decision_id"], "d2")
        self.assertTrue(case.settlement_audit()["ok"])


class VisibilityTest(unittest.TestCase):
    def test_party_sees_only_own_slice(self):
        case = make_case()
        case.add_item("i-mer", "商户退款", MERCHANT, ACTION_REFUND)
        case.add_item("i-ben", "权益补发", BENEFIT, ACTION_REISSUE_BENEFIT)
        case.submit_evidence("ev-mer", EVIDENCE_TRANSACTION_PROOF, "h-mer", CONSUMER, T0,
                             item_id="i-mer")
        case.submit_evidence("ev-ben", EVIDENCE_TRANSACTION_PROOF, "h-ben", CONSUMER, T0,
                             item_id="i-ben")
        case.record_commitment("c-mer", MERCHANT, "承诺场馆现场退差价", evidence_id="ev-mer")
        case.record_commitment("c-ben", BENEFIT, "承诺补发观赛权益", evidence_id="ev-ben")

        merchant_view = case.view_for(MERCHANT)
        self.assertEqual({i["item_id"] for i in merchant_view["items"]}, {"i-mer"})
        self.assertEqual({e["evidence_id"] for e in merchant_view["evidence"]}, {"ev-mer"})
        self.assertEqual({c["commitment_id"] for c in merchant_view["commitments"]}, {"c-mer"})
        decision_ids = {d["decision_id"] for d in merchant_view["decisions"]}
        self.assertEqual(decision_ids, set())  # 尚无决定
        # 时间线同样按方裁剪。
        timeline_items = {
            e.get("item_id") for e in merchant_view["timeline"] if e.get("item_id")
        }
        self.assertEqual(timeline_items, {"i-mer"})

        benefit_view = case.view_for(BENEFIT)
        self.assertEqual({i["item_id"] for i in benefit_view["items"]}, {"i-ben"})
        self.assertEqual({c["commitment_id"] for c in benefit_view["commitments"]}, {"c-ben"})

    def test_operator_sees_full_case(self):
        case = make_case()
        case.add_item("i-mer", "商户退款", MERCHANT, ACTION_REFUND)
        view = case.view_for(OPERATOR)
        self.assertEqual({i["item_id"] for i in view["items"]}, {"i-mer"})
        self.assertEqual(view["case"]["case_id"], "case-1")


class RecoveryTest(unittest.TestCase):
    def _path(self):
        return Path(tempfile.mkdtemp()) / "events.jsonl"

    def test_rebuild_from_jsonl_restores_state_and_idempotency(self):
        path = self._path()
        store = FileEventStore(path)
        case = JointCase.open(
            "case-1", "CMP-1", CONSUMER, PARTICIPANTS, store=store, clock=FixedClock(T0),
        )
        case.submit_evidence("ev-1", EVIDENCE_TRANSACTION_PROOF, "h1", CONSUMER, T0)
        case.add_item("i1", "退款", MERCHANT, ACTION_REFUND, due_at=T0 + timedelta(hours=24))
        case.claim_item("i1", MERCHANT)
        case.propose_decision("i1", "d1", ACTION_REFUND, "80.00", MERCHANT, basis=["ev-1"])
        case.accept_decision("i1", "d1", CONSUMER, command_id="cmd-accept-1")

        # 模拟服务重启：新建聚合、重放日志。
        restored = JointCase.rebuild(FileEventStore(path), clock=FixedClock(T0))
        self.assertEqual(restored.case_id, "case-1")
        self.assertEqual(restored.state["items"]["i1"]["status"], ITEM_CLAIMED)
        self.assertEqual(restored.state["decisions"]["d1"]["status"], DECISION_ACCEPTED)
        self.assertEqual(len(restored.state["timeline"]), 6)

        # 重放后命令幂等键仍然生效，重复投递同一接受命令被拒。
        with self.assertRaises(CaseError) as raised:
            restored.accept_decision("i1", "d1", CONSUMER, command_id="cmd-accept-1")
        self.assertEqual(raised.exception.code, "DUPLICATE_COMMAND")
        # 迟到证据仍然幂等。
        before = restored.state["sequence"]
        restored.submit_evidence("ev-1", EVIDENCE_TRANSACTION_PROOF, "h1", CONSUMER, T0)
        self.assertEqual(restored.state["sequence"], before)

        # 继续走完流程并结算。
        restored.fulfill_remedy("i1", "pay-1", MERCHANT, now=T0 + timedelta(hours=2))
        restored.create_settlement_batch("B-1", OPERATOR)
        restored.confirm_batch("B-1", OPERATOR)
        restored.close_case()
        self.assertTrue(restored.settlement_audit()["ok"])

    def test_jsonl_is_append_only_with_one_json_event_per_line(self):
        path = self._path()
        store = FileEventStore(path)
        case = JointCase.open("case-9", "CMP-9", CONSUMER, PARTICIPANTS,
                              store=store, clock=FixedClock(T0))
        case.add_item("i1", "退款", MERCHANT, ACTION_REFUND)
        lines = path.read_text(encoding="utf-8").strip().splitlines()
        self.assertEqual(len(lines), 2)
        for line in lines:
            payload = json.loads(line)
            self.assertIn("type", payload)
            self.assertIn("seq", payload)

    def test_escalation_after_restart_does_not_double_fire(self):
        path = self._path()
        store = FileEventStore(path)
        case = JointCase.open("case-1", "CMP-1", CONSUMER, PARTICIPANTS,
                              store=store, clock=FixedClock(T0))
        case.add_item("i1", "退款", MERCHANT, ACTION_REFUND, due_at=T0 + timedelta(hours=1))
        case.sweep(now=T0 + timedelta(hours=2))
        self.assertEqual(len(case.state["items"]["i1"]["escalations"]), 1)

        restored = JointCase.rebuild(FileEventStore(path), clock=FixedClock(T0 + timedelta(hours=3)))
        fired = restored.sweep()
        self.assertEqual(fired, [])
        self.assertEqual(len(restored.state["items"]["i1"]["escalations"]), 1)


class TimelineExplainTest(unittest.TestCase):
    def test_timeline_records_original_basis_and_override_history(self):
        case = make_case()
        case.add_item("i1", "退款", MERCHANT, ACTION_REFUND)
        case.claim_item("i1", MERCHANT)
        case.submit_evidence("ev-1", EVIDENCE_TRANSACTION_PROOF, "h1", CONSUMER, T0)
        case.propose_decision("i1", "d1", ACTION_REFUND, "80.00", MERCHANT, basis=["ev-1"])
        case.accept_decision("i1", "d1", CONSUMER)
        case.propose_override("i1", "d2", ACTION_REFUND, "120.00",
                              reason="承诺凭证迟到", by=OPERATOR, basis=["ev-1"])
        case.accept_decision("i1", "d2", CONSUMER)
        case.fulfill_remedy("i1", "pay-d2", MERCHANT)

        types = [e["type"] for e in case.state["timeline"]]
        self.assertIn("OVERRIDE_PROPOSED", types)
        self.assertIn("DECISION_ACCEPTED", types)
        d1_event = next(e for e in case.state["timeline"]
                        if e["type"] == "DECISION_PROPOSED" and e["decision_id"] == "d1")
        self.assertEqual(d1_event["basis"], ["ev-1"])

        explanation = case.explain()
        item = next(i for i in explanation["items"] if i["item_id"] == "i1")
        self.assertEqual(item["status"], ITEM_COMPLETED)
        self.assertEqual(item["amount"], "120.00")
        self.assertEqual(item["next_step"], "已完成，等待结算")
        self.assertTrue(explanation["money_conservation_ok"])
        self.assertEqual(explanation["unbatched_total"], {"CNY": "120.00"})


if __name__ == "__main__":
    unittest.main()
