"""只读投影：客服时间线、参与方最小可见视图、金额守恒汇总。

投影不产生事件、不写存储，全部由案件当前状态派生。访问控制在这里
第二次落地（服务层做命令授权，这里做读过滤）：未经授权的参与方只能
看到自己负责的责任项、自己提交的证据、分派给自己的动作，以及判罚中
属于自己的那一档金额。
"""

from __future__ import annotations

from typing import Any

from .domain import (
    CONSUMER,
    FUND_ACTION_KINDS,
    ORGANIZER,
    PLATFORM,
    MERCHANT,
    RESPONSIBLE_ROLES,
    Case,
)

ARBITER = "arbiter"
REGULATOR = "regulator"
FINANCE = "finance"
AGENT = "agent"

#: 可查看全案的角色：客服、主办方、仲裁、监管
FULL_VIEW_ROLES = {AGENT, ORGANIZER, ARBITER, REGULATOR, CONSUMER}

# 事件类型 -> 时间线条目的中文说明
_TIMELINE_LABELS = {
    "CaseOpened": "案件建立",
    "EvidenceSubmitted": "证据到达",
    "LiabilityItemAdded": "责任项建立",
    "LiabilityClaimed": "责任项认领",
    "DecisionRecorded": "判罚记录",
    "ConsumerOffered": "消费者选项发出",
    "ConsumerChose": "消费者作出选择",
    "ActionAssigned": "补救动作分派",
    "ActionCompleted": "补救动作完成",
    "ItemEscalated": "责任项超时升级",
    "CaseClosed": "案件结案",
    "CaseReopened": "案件因关键证据重审",
    "PersonalDataRedacted": "个人材料撤回",
    "SettlementBatchCreated": "结算批次生成",
    "SettlementBatchConfirmed": "结算批次确认",
}


# ---------------------------------------------------------------------------
# 客服时间线
# ---------------------------------------------------------------------------

def timeline(case: Case) -> list[dict[str, Any]]:
    """按事件序号生成案件时间线，供客服向消费者解释"当前责任/下一步"。"""
    entries: list[dict[str, Any]] = []
    for event in case.events:
        et = event["event_type"]
        d = event.get("data", {})
        entries.append({
            "seq": event["seq"],
            "at": event["occurred_at"],
            "actor": event["actor"],
            "type": et,
            "label": _TIMELINE_LABELS.get(et, et),
            "detail": _timeline_detail(case, et, d),
        })
    return entries


def _timeline_detail(case: Case, et: str, d: dict) -> dict[str, Any]:
    if et == "EvidenceSubmitted":
        return {
            "evidence_id": d["evidence_id"],
            "source_party": d["source_party"],
            "kind": d["kind"],
            "redacted": case.evidences[d["evidence_id"]].redacted
            if d["evidence_id"] in case.evidences else False,
        }
    if et == "LiabilityClaimed":
        return {"item_id": d["item_id"], "claimed_by": d["party_id"]}
    if et == "DecisionRecorded":
        detail = {
            "item_id": d["item_id"],
            "decision_id": d["decision_id"],
            "total_cents": d["compensation_total_cents"],
            "shares_cents": d["shares_cents"],
            "supersedes_event_seq": d.get("supersedes_event_seq"),
            "revision": "改判" if d.get("supersedes_event_seq") is not None else "初判",
        }
        return detail
    if et == "ActionAssigned":
        return {
            "action_id": d["action_id"], "item_id": d["item_id"],
            "kind": d["kind"], "responsible_party": d["responsible_party"],
            "deadline": d["deadline"], "amount_cents": d.get("amount_cents", 0),
        }
    if et == "ActionCompleted":
        return {
            "action_id": d["action_id"], "item_id": d["item_id"],
            "amount_cents": d.get("amount_cents", 0), "result_ref": d.get("result_ref"),
        }
    if et == "ItemEscalated":
        return {
            "item_id": d["item_id"], "action_id": d["action_id"],
            "reason": d["reason"], "new_deadline": d.get("new_deadline"),
        }
    if et == "ConsumerChose":
        return {"offer_id": d["offer_id"], "option_id": d.get("option_id"),
                "accepted": d["accepted"]}
    if et in ("SettlementBatchCreated", "SettlementBatchConfirmed"):
        return {"batch_id": d["batch_id"], "total_cents": d.get("total_cents")}
    return dict(d)


# ---------------------------------------------------------------------------
# 当前责任 / 下一步 / 金额守恒
# ---------------------------------------------------------------------------

def case_summary(case: Case) -> dict[str, Any]:
    """给客服的稳定答复底稿：责任现状、下一步、金额守恒。"""
    items = []
    for item in case.items.values():
        decision = item.decisions[-1] if item.decisions else None
        pending_actions = [
            _action_brief(case.actions[aid])
            for aid in item.actions if case.actions[aid].status != "COMPLETED"
        ]
        items.append({
            "item_id": item.item_id,
            "title": item.title,
            "status": item.status,
            "claimed_by": item.claimed_by,
            "escalation_level": item.escalation_level,
            "current_decision": None if decision is None else {
                "decision_id": decision.decision_id,
                "decided_by": decision.decided_by,
                "reason": decision.reason,
                "total_cents": decision.compensation_total_cents,
                "shares_cents": decision.shares_cents,
                "event_seq": decision.event_seq,
                "revisions": len(item.decisions),
            },
            "pending_actions": pending_actions,
        })

    money = money_conservation(case)
    next_steps = _next_steps(case)
    return {
        "case_id": case.case_id,
        "status": case.status,
        "currency": case.currency,
        "items": items,
        "consumer": {
            "consumer_id": case.consumer_id,
            "offer": None if case.current_offer is None else {
                "offer_id": case.current_offer.offer_id,
                "chosen_option_id": case.current_offer.chosen_option_id,
                "accepted": case.current_offer.accepted,
                "valid_until": case.current_offer.valid_until.isoformat(),
            },
        },
        "money": money,
        "next_steps": next_steps,
        "closed_at": case.closed_at.isoformat() if case.closed_at else None,
    }


def money_conservation(case: Case) -> dict[str, Any]:
    """金额守恒视图。

    - ``decided_total_cents``：各责任项**当前有效**判罚补偿之和；
    - ``assigned_total_cents``：已分派资金动作金额之和（<= 判罚总额）；
    - ``settled_total_cents``：消费者已实际到账金额；
    - ``batched_total_cents``：已进入结算批次的金额；
    - ``shares_by_party``：按参与方汇总当前判罚的分摊金额；
    - ``balanced``：守恒是否成立（判罚 >= 分派 >= 已结算 >= 已批次）。
    """
    decided = 0
    shares: dict[str, int] = {}
    for item in case.items.values():
        if item.decisions:
            current = item.decisions[-1]
            decided += current.compensation_total_cents
            for party, cents in current.shares_cents.items():
                shares[party] = shares.get(party, 0) + cents

    fund_actions = [a for a in case.actions.values() if a.kind in FUND_ACTION_KINDS]
    assigned = sum(a.amount_cents for a in fund_actions)
    settled = sum(a.amount_cents for a in fund_actions if a.status == "COMPLETED")
    batched = sum(
        line["amount_cents"]
        for batch in case.settlement_batches for line in batch["lines"]
    )
    balanced = decided >= assigned >= settled >= batched and sum(shares.values()) == decided
    return {
        "decided_total_cents": decided,
        "assigned_total_cents": assigned,
        "settled_total_cents": settled,
        "batched_total_cents": batched,
        "shares_by_party": shares,
        "balanced": balanced,
    }


def _next_steps(case: Case) -> list[dict[str, Any]]:
    if case.status == "CLOSED":
        return [{"step": "案件已结案", "owner": None}]
    steps: list[dict[str, Any]] = []
    for item in case.items.values():
        if not item.decisions:
            steps.append({
                "step": "等待判罚",
                "owner": item.claimed_by or "待认领",
                "item_id": item.item_id,
            })
        for aid in item.actions:
            action = case.actions[aid]
            if action.status != "COMPLETED":
                steps.append({
                    "step": f"完成{action.kind}",
                    "owner": action.responsible_party,
                    "item_id": item.item_id,
                    "action_id": action.action_id,
                    "deadline": action.deadline.isoformat(),
                    "escalated_once": action.escalated,
                })
    offer = case.current_offer
    if offer is not None and offer.chose_at is None:
        steps.append({"step": "等待消费者选择补救方式", "owner": CONSUMER,
                      "offer_id": offer.offer_id,
                      "valid_until": offer.valid_until.isoformat()})
    if not steps:
        steps.append({"step": "全部必需动作已完成，可结案", "owner": ORGANIZER})
    return steps


def _action_brief(action) -> dict[str, Any]:
    return {
        "action_id": action.action_id,
        "kind": action.kind,
        "responsible_party": action.responsible_party,
        "deadline": action.deadline.isoformat(),
        "amount_cents": action.amount_cents,
        "escalated": action.escalated,
    }


# ---------------------------------------------------------------------------
# 参与方最小可见视图
# ---------------------------------------------------------------------------

def party_view(case: Case, viewer: str) -> dict[str, Any]:
    """参与方视角的案件数据，只含其被授权看到的部分。

    - 客服/消费者/主办方/仲裁/监管：全案（消费者视图会隐去其他参与方内部分摊）；
    - 商户/权益平台：仅自己认领的责任项、自己提交的证据、分派给自己的动作、
      判罚中属于自己的分摊金额；
    - 财务：仅结算批次与对应资金动作。

    其他未知身份一律得到最小占位视图（案件存在性 + 空集合），不报错泄露。
    """
    if viewer in FULL_VIEW_ROLES or viewer.startswith(f"{CONSUMER}:"):
        view = case_summary(case)
        view["timeline"] = timeline(case)
        if viewer == CONSUMER or viewer.startswith(f"{CONSUMER}:"):
            view = _consumer_perspective(view)
        return view

    if viewer in RESPONSIBLE_ROLES:
        return _responsible_party_view(case, viewer)

    if viewer == FINANCE:
        return _finance_view(case)

    return {
        "case_id": case.case_id,
        "status": case.status,
        "visible": False,
        "items": [],
        "evidences": [],
        "actions": [],
        "message": "该身份未获授权查看本案详情",
    }


def _responsible_party_view(case: Case, viewer: str) -> dict[str, Any]:
    visible_items = {
        iid: item for iid, item in case.items.items()
        if item.claimed_by == viewer
    }
    evidences = []
    for evidence in case.evidences.values():
        if evidence.source_party == viewer:
            evidences.append({
                "evidence_id": evidence.evidence_id,
                "kind": evidence.kind,
                "submitted_at": evidence.submitted_at.isoformat(),
                "content": None if evidence.redacted else evidence.content,
                "redacted": evidence.redacted,
            })
    actions = []
    decisions = []
    for iid, item in visible_items.items():
        for aid in item.actions:
            action = case.actions[aid]
            if action.responsible_party == viewer:
                actions.append(_action_brief(action))
        for decision in item.decisions:
            decisions.append({
                "item_id": iid,
                "decision_id": decision.decision_id,
                "total_cents": decision.compensation_total_cents,
                "my_share_cents": decision.shares_cents.get(viewer, 0),
                "reason": decision.reason,
                "basis_evidence": decision.basis_evidence,
                "event_seq": decision.event_seq,
            })
    return {
        "case_id": case.case_id,
        "status": case.status,
        "viewer": viewer,
        "visible": True,
        "items": [
            {"item_id": item.item_id, "title": item.title, "status": item.status,
             "escalation_level": item.escalation_level}
            for item in visible_items.values()
        ],
        "evidences": evidences,
        "actions": actions,
        "decisions": decisions,
    }


def _finance_view(case: Case) -> dict[str, Any]:
    fund_actions = {}
    for batch in case.settlement_batches:
        for line in batch["lines"]:
            fund_actions[line["action_id"]] = {
                "action_id": line["action_id"],
                "item_id": line["item_id"],
                "responsible_party": line["responsible_party"],
                "kind": line["kind"],
                "amount_cents": line["amount_cents"],
                "completed_at": line["completed_at"],
            }
    return {
        "case_id": case.case_id,
        "status": case.status,
        "viewer": FINANCE,
        "currency": case.currency,
        "confirmed_batches": sorted(case.confirmed_batches),
        "batches": [
            {
                "batch_id": b["batch_id"],
                "total_cents": b["total_cents"],
                "created_at": b["created_at"],
                "confirmed": b["batch_id"] in case.confirmed_batches,
                "lines": b["lines"],
            }
            for b in case.settlement_batches
        ],
        "available_for_next_batch": [
            {
                "action_id": a.action_id,
                "responsible_party": a.responsible_party,
                "amount_cents": a.amount_cents,
            }
            for a in case.unsettled_fund_actions()
        ],
    }


def _consumer_perspective(view: dict[str, Any]) -> dict[str, Any]:
    """消费者不需要看参与方内部分摊，只看自己拿到/将拿到什么。"""
    for item in view.get("items", []):
        decision = item.get("current_decision")
        if decision is not None:
            decision.pop("shares_cents", None)
    return view
