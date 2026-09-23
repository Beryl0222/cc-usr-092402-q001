"""联合处置测试共用构造工具。"""

from __future__ import annotations

from datetime import timedelta

from src.jointcase import clock
from src.jointcase.service import CaseService
from src.jointcase.store import EventStore


def fresh_service(tmp_path) -> CaseService:
    return CaseService(EventStore(str(tmp_path)))


def build_two_item_case(tmp_path, case_id: str = "C1", t0=None) -> dict:
    """构造一个含两个责任项、尚未结案的典型跨城赛事投诉案件。

    场景：商户曾承诺退一赔三（责任项 I1，商户认领）；权益平台核销了
    未使用的接驳权益（责任项 I2，平台认领）。
    """
    svc = fresh_service(tmp_path)
    t0 = t0 or clock.now_utc()
    deadline = clock.deadline_after(t0, timedelta(hours=48))

    svc.create_case(case_id=case_id, actor="agent", complaint_ref="COMP-1",
                    transaction_ref="TX-1", consumer_id="consumer-9", at=t0)
    svc.submit_evidence(case_id, actor="agent", evidence_id="E-MERCHANT",
                        source_party="merchant", kind="MERCHANT_COMMITMENT",
                        content={"commitment": "退一赔三", "channel": "现场海报"},
                        contains_personal_data=True, at=t0)
    svc.submit_evidence(case_id, actor="agent", evidence_id="E-PLATFORM",
                        source_party="platform", kind="BENEFIT_REDEEMED",
                        content={"benefit": "接驳券", "status": "未使用已核销"}, at=t0)
    svc.submit_evidence(case_id, actor="agent", evidence_id="E-TX",
                        source_party="consumer", kind="TRANSACTION_VOUCHER",
                        content={"paid_cents": 6000}, at=t0)

    svc.add_item(case_id, actor="agent", item_id="I1",
                 title="商户退一赔三承诺未兑现", linked_evidence=["E-MERCHANT", "E-TX"])
    svc.add_item(case_id, actor="agent", item_id="I2",
                 title="权益平台错误核销接驳券", linked_evidence=["E-PLATFORM"])

    svc.claim_item(case_id, party_id="merchant", item_id="I1")
    svc.claim_item(case_id, party_id="platform", item_id="I2")

    svc.decide(case_id, actor="merchant", item_id="I1", decision_id="D1-1",
               reason="现场海报承诺凭证成立", basis_evidence=["E-MERCHANT"],
               shares_cents={"merchant": 3000, "organizer": 1000}, at=t0)
    svc.decide(case_id, actor="platform", item_id="I2", decision_id="D2-1",
               reason="核销记录与未使用事实矛盾", basis_evidence=["E-PLATFORM"],
               shares_cents={"platform": 2000}, at=t0)

    svc.assign_action(case_id, actor="merchant", action_id="A1", item_id="I1",
                      kind="REFUND", responsible_party="merchant",
                      deadline=deadline, amount_cents=3000)
    svc.assign_action(case_id, actor="merchant", action_id="A2", item_id="I1",
                      kind="TOPUP_REFUND", responsible_party="organizer",
                      deadline=deadline, amount_cents=1000)
    svc.assign_action(case_id, actor="platform", action_id="A3", item_id="I2",
                      kind="REPLACEMENT_BENEFIT", responsible_party="platform",
                      deadline=deadline)

    svc.offer_choices(case_id, actor="organizer", offer_id="O1", valid_until=deadline,
                      options=[
                          {"option_id": "OPT-REFUND", "kind": "PARTIAL_REFUND",
                           "label": "接受部分退款", "amount_cents": 4000},
                          {"option_id": "OPT-BENEFIT", "kind": "REPLACEMENT_BENEFIT",
                           "label": "改领等值赛事权益", "benefit_ref": "BENEFIT-NEW"},
                      ])
    return {
        "svc": svc,
        "store": svc.store,
        "t0": t0,
        "deadline": deadline,
        "case_id": case_id,
        "items": ["I1", "I2"],
        "actions": {"I1": ["A1", "A2"], "I2": ["A3"]},
    }
