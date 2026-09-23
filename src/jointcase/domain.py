"""联合处置案件领域模型（事件溯源）。

案件（:class:`Case`）是聚合根。所有状态变化都表现为追加的事件；
当前状态由事件回放得到，因此天然支持：

- 相同证据迟到/重复（提交幂等，按 ``source + evidence_id`` 去重）；
- 人工改判（新决策事件显式标注被替代事件，旧版本永不删除）；
- 故障恢复（重放事件日志即可重建内存状态）；
- 金额守恒（每次回放后可校验不变量）。

命令方法只**产生事件**（``(event_type, data)`` 元组），不自行追加序号、
不写存储；序号分配与并发控制由 :mod:`src.jointcase.store` 与
:mod:`src.jointcase.service` 负责。

金额一律使用整数最小货币单位（分），杜绝浮点误差。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .clock import is_overdue, now_utc, parse_ts

# ---------------------------------------------------------------------------
# 角色与常量
# ---------------------------------------------------------------------------

ORGANIZER = "organizer"          # 主办方
MERCHANT = "merchant"            # 场馆商户
PLATFORM = "platform"            # 权益平台
CONSUMER = "consumer"            # 消费者
ARBITER = "arbiter"              # 仲裁/监管协同角色
REGULATOR = "regulator"

#: 可被分派责任的参与方角色
RESPONSIBLE_ROLES = (ORGANIZER, MERCHANT, PLATFORM)

#: 消费者可选补救类型
PARTIAL_REFUND = "PARTIAL_REFUND"
REPLACEMENT_BENEFIT = "REPLACEMENT_BENEFIT"
OPTION_KINDS = (PARTIAL_REFUND, REPLACEMENT_BENEFIT)

#: 动作类型；FUND_ACTION_KINDS 会进入资金结算
REFUND = "REFUND"
TOPUP_REFUND = "TOPUP_REFUND"
CLARIFICATION = "CLARIFICATION"
APOLOGY = "APOLOGY"
FUND_ACTION_KINDS = (REFUND, TOPUP_REFUND)
BENEFIT_ACTION = "REPLACEMENT_BENEFIT"
ACTION_KINDS = FUND_ACTION_KINDS + (BENEFIT_ACTION, CLARIFICATION, APOLOGY)

#: 案件状态
OPEN = "OPEN"
REOPENED = "REOPENED"
CLOSED = "CLOSED"

#: 责任项状态
ITEM_OPEN = "OPEN"
ITEM_DECIDED = "DECIDED"
ITEM_ESCALATED = "ESCALATED"


class JointCaseError(Exception):
    """联合处置领域错误基类。"""


class NotFound(JointCaseError):
    """引用的案件对象不存在。"""


class RuleViolation(JointCaseError):
    """命令违反案件处置规则（不变量）。"""


class AlreadyClaimed(JointCaseError):
    """责任项已被其他参与方认领（并发认领的失败方）。"""


class CaseClosedError(JointCaseError):
    """案件已结案，该命令不允许在结案后执行。"""


class Conflict(JointCaseError):
    """追加事件时序号已被他人抢先（乐观锁冲突），调用方应重放后重试。"""


# ---------------------------------------------------------------------------
# 状态数据结构
# ---------------------------------------------------------------------------

@dataclass
class Evidence:
    evidence_id: str
    source_party: str
    kind: str
    dedup_key: str
    submitted_at: datetime
    contains_personal_data: bool = False
    content: dict[str, Any] | None = None
    content_hash: str | None = None
    redacted: bool = False
    redacted_at: datetime | None = None
    material: bool = False  # 关键证据：结案后到达会触发重审


@dataclass
class Decision:
    """一次判罚；改判时旧版本仍保留在 ``LiabilityItem.decisions`` 中。"""

    event_seq: int
    decision_id: str
    decided_by: str
    reason: str
    basis_evidence: list[str]
    shares_cents: dict[str, int]
    compensation_total_cents: int
    supersedes_event_seq: int | None = None
    decided_at: datetime | None = None


@dataclass
class Action_:
    action_id: str
    item_id: str
    kind: str
    responsible_party: str
    deadline: datetime
    amount_cents: int = 0
    status: str = "ASSIGNED"   # ASSIGNED / COMPLETED
    completed_at: datetime | None = None
    result_ref: str | None = None
    escalated: bool = False
    deadline_history: list[datetime] = field(default_factory=list)


@dataclass
class LiabilityItem:
    item_id: str
    title: str
    linked_evidence: list[str] = field(default_factory=list)
    claimed_by: str | None = None
    claimed_at: datetime | None = None
    decisions: list[Decision] = field(default_factory=list)
    actions: list[str] = field(default_factory=list)
    status: str = ITEM_OPEN
    escalation_level: int = 0


@dataclass
class OfferOption:
    option_id: str
    kind: str
    label: str
    amount_cents: int = 0
    benefit_ref: str | None = None


@dataclass
class Offer:
    offer_id: str
    options: list[OfferOption]
    issued_at: datetime
    valid_until: datetime
    chosen_option_id: str | None = None
    accepted: bool | None = None
    chose_at: datetime | None = None


# ---------------------------------------------------------------------------
# 聚合根
# ---------------------------------------------------------------------------

class Case:
    """联合处置案件聚合根，通过事件回放构建。"""

    def __init__(self, case_id: str, complaint_ref: str, transaction_ref: str,
                 consumer_id: str, organizer_id: str, currency: str = "CNY"):
        self.case_id = case_id
        self.complaint_ref = complaint_ref
        self.transaction_ref = transaction_ref
        self.consumer_id = consumer_id
        self.organizer_id = organizer_id
        self.currency = currency

        self.status = OPEN
        self.evidences: dict[str, Evidence] = {}
        self._dedup: set[str] = set()
        self.items: dict[str, LiabilityItem] = {}
        self.offers: list[Offer] = []
        self.actions: dict[str, Action_] = {}
        self.events: list[dict[str, Any]] = []
        self.closed_at: datetime | None = None
        self.reopen_count = 0
        self.settlement_batches: list[dict[str, Any]] = []
        self.confirmed_batches: set[str] = set()
        self.batched_action_ids: set[str] = set()

    # -- 回放 --------------------------------------------------------------

    @classmethod
    def replay(cls, events: list[dict[str, Any]]) -> "Case":
        if not events or events[0]["event_type"] != "CaseOpened":
            raise RuleViolation("案件事件流必须以 CaseOpened 开始")
        d = events[0]["data"]
        case = cls(
            case_id=events[0]["case_id"],
            complaint_ref=d["complaint_ref"],
            transaction_ref=d["transaction_ref"],
            consumer_id=d["consumer_id"],
            organizer_id=d.get("organizer_id", "organizer"),
            currency=d.get("currency", "CNY"),
        )
        for event in events:
            case.apply(event)
        return case

    @property
    def seq(self) -> int:
        """下一个事件的期望序号（等于已存在事件数）。"""
        return len(self.events)

    @property
    def current_offer(self) -> Offer | None:
        return self.offers[-1] if self.offers else None

    def item_decisions(self, item_id: str) -> list[Decision]:
        return self._item(item_id).decisions

    def current_decision(self, item_id: str) -> Decision | None:
        decisions = self._item(item_id).decisions
        return decisions[-1] if decisions else None

    # -- 命令：均返回 (event_type, data)，不直接修改状态 -------------------

    def open_case(self, *, at: datetime | None = None) -> tuple[str, dict]:
        """构造 CaseOpened（由仓储首次建案时使用）。"""
        return "CaseOpened", {
            "complaint_ref": self.complaint_ref,
            "transaction_ref": self.transaction_ref,
            "consumer_id": self.consumer_id,
            "organizer_id": self.organizer_id,
            "currency": self.currency,
            "opened_at": (at or now_utc()).isoformat(),
        }

    def submit_evidence(self, *, evidence_id: str, source_party: str, kind: str,
                        content: dict | None = None, content_hash: str | None = None,
                        contains_personal_data: bool = False, material: bool = False,
                        submitted_at: datetime | None = None) -> list[tuple[str, dict]]:
        """提交证据（任何阶段都允许到达，包括结案后的迟到证据）。

        - 重复提交（同 source_party + evidence_id）返回空列表，
          调用方不追加事件——这是迟到/重复证据的幂等点；
        - 结案后到达的**关键证据**（material=True）会同时产生 CaseReopened，
          案件回到可处置状态；非关键证据照常收录备查，但不推翻结案。
        """
        dedup_key = f"{source_party}:{evidence_id}"
        if dedup_key in self._dedup:
            return []
        ts = (submitted_at or now_utc()).isoformat()
        data = {
            "evidence_id": evidence_id,
            "source_party": source_party,
            "kind": kind,
            "dedup_key": dedup_key,
            "submitted_at": ts,
            "contains_personal_data": bool(contains_personal_data),
            "material": bool(material),
            "content": content,
            "content_hash": content_hash,
        }
        events: list[tuple[str, dict]] = [("EvidenceSubmitted", data)]
        if self.status == CLOSED and material:
            events.append(("CaseReopened", {
                "reason": "结案后关键证据到达",
                "evidence_id": evidence_id,
                "reopened_at": ts,
            }))
        return events  # type: ignore[return-value]

    def add_item(self, *, item_id: str, title: str,
                 linked_evidence: list[str] | None = None) -> tuple[str, dict]:
        self._require_open()
        if item_id in self.items:
            raise RuleViolation(f"责任项已存在: {item_id}")
        linked = linked_evidence or []
        unknown = [e for e in linked if e not in self.evidences]
        if unknown:
            raise RuleViolation(f"关联证据不存在: {unknown}")
        return "LiabilityItemAdded", {
            "item_id": item_id, "title": title, "linked_evidence": linked,
        }

    def claim_item(self, *, item_id: str, party_id: str,
                   at: datetime | None = None) -> tuple[str, dict]:
        """并发认领：只有第一个成功追加的参与方生效。"""
        self._require_open()
        item = self._item(item_id)
        if item.claimed_by is not None:
            raise AlreadyClaimed(
                f"责任项 {item_id} 已被 {item.claimed_by} 认领"
            )
        if party_id not in RESPONSIBLE_ROLES and party_id != self.organizer_id:
            raise RuleViolation(f"参与方无资格认领责任项: {party_id}")
        return "LiabilityClaimed", {
            "item_id": item_id,
            "party_id": party_id,
            "claimed_at": (at or now_utc()).isoformat(),
        }

    def decide(self, *, item_id: str, decision_id: str, decided_by: str, reason: str,
               shares_cents: dict[str, int], basis_evidence: list[str] | None = None,
               supersedes_event_seq: int | None = None,
               at: datetime | None = None) -> tuple[str, dict]:
        """记录判罚；再次判罚即**改判**，必须显式标注被替代的决策事件序号。

        金额守恒：分摊金额之和必须等于补偿总额；已经实际支付给消费者的
        金额不得因改判而减少（已接受的补救不能失效）。
        """
        self._require_open()
        item = self._item(item_id)
        total = sum(shares_cents.values())
        if total <= 0:
            raise RuleViolation("判罚补偿总额必须为正数")
        if any(cents < 0 for cents in shares_cents.values()):
            raise RuleViolation("责任分摊金额不能为负")
        unknown = [e for e in (basis_evidence or []) if e not in self.evidences]
        if unknown:
            raise RuleViolation(f"判罚依据证据不存在: {unknown}")
        current = self.current_decision(item_id)
        if current is not None:
            if supersedes_event_seq != current.event_seq:
                raise RuleViolation(
                    "改判必须通过 supersedes_event_seq 标注被替代的当前决策版本 "
                    f"(当前版本 seq={current.event_seq})，原决定与依据将被保留"
                )
        settled = self.settled_cents_for_item(item_id)
        if total < settled:
            raise RuleViolation(
                f"改判后总额 {total} 低于消费者已实际获得金额 {settled}："
                "已接受的补救不能失效"
            )
        return "DecisionRecorded", {
            "item_id": item_id,
            "decision_id": decision_id,
            "decided_by": decided_by,
            "reason": reason,
            "basis_evidence": basis_evidence or [],
            "shares_cents": dict(shares_cents),
            "compensation_total_cents": total,
            "supersedes_event_seq": supersedes_event_seq,
            "decided_at": (at or now_utc()).isoformat(),
        }

    def offer_choices(self, *, offer_id: str, options: list[dict],
                      valid_until: datetime,
                      issued_at: datetime | None = None) -> tuple[str, dict]:
        """向消费者提供部分退款 / 替代权益等可选项。"""
        self._require_open()
        if self.current_offer is not None and self.current_offer.chose_at is None:
            raise RuleViolation("上一选项尚未得到消费者选择，不能重复发出")
        normalized: list[dict] = []
        for opt in options:
            if opt["kind"] not in OPTION_KINDS:
                raise RuleViolation(f"未知消费者选项类型: {opt['kind']}")
            amount = int(opt.get("amount_cents", 0))
            if opt["kind"] == PARTIAL_REFUND and amount <= 0:
                raise RuleViolation("退款选项必须给出正数金额")
            normalized.append({
                "option_id": opt["option_id"],
                "kind": opt["kind"],
                "label": opt.get("label", opt["kind"]),
                "amount_cents": amount,
                "benefit_ref": opt.get("benefit_ref"),
            })
        if not normalized:
            raise RuleViolation("至少需要一个消费者选项")
        return "ConsumerOffered", {
            "offer_id": offer_id,
            "options": normalized,
            "issued_at": (issued_at or now_utc()).isoformat(),
            "valid_until": parse_ts(valid_until).isoformat(),
        }

    def consumer_choose(self, *, offer_id: str, option_id: str, accepted: bool,
                        at: datetime | None = None) -> tuple[str, dict]:
        """消费者作出选择。选择一旦作出即为终局，不可反复。"""
        self._require_open()
        offer = self._find_open_offer(offer_id)
        if offer.chosen_option_id is not None:
            raise RuleViolation("消费者已作出选择，不能重复选择或撤回")
        if accepted and not any(o.option_id == option_id for o in offer.options):
            raise RuleViolation(f"选项不在该次 offer 中: {option_id}")
        return "ConsumerChose", {
            "offer_id": offer_id,
            "option_id": option_id if accepted else None,
            "accepted": accepted,
            "chose_at": (at or now_utc()).isoformat(),
        }

    def assign_action(self, *, action_id: str, item_id: str, kind: str,
                      responsible_party: str, deadline: datetime,
                      amount_cents: int = 0) -> tuple[str, dict]:
        """为责任项分配必需动作并启动处理时钟。"""
        self._require_open()
        item = self._item(item_id)
        if action_id in self.actions:
            raise RuleViolation(f"动作已存在: {action_id}")
        if kind not in ACTION_KINDS:
            raise RuleViolation(f"未知动作类型: {kind}")
        if kind in FUND_ACTION_KINDS:
            if amount_cents <= 0:
                raise RuleViolation("资金类动作必须给出正数金额")
            # 金额守恒：该责任项资金动作不得超过当前判罚总额；
            # 已完成终局补救后只允许就差额补发，杜绝重复退款。
            decision = self.current_decision(item_id)
            if decision is None:
                raise RuleViolation("责任项尚未判罚，不能分派资金动作")
            pending = sum(
                a.amount_cents for a in self._item_actions(item_id)
                if a.kind in FUND_ACTION_KINDS
            )
            if pending + amount_cents > decision.compensation_total_cents:
                raise RuleViolation(
                    f"资金动作金额 {pending + amount_cents} 超过判罚总额 "
                    f"{decision.compensation_total_cents}：金额必须守恒"
                )
            settled_kind = any(
                a.kind == REFUND and a.status == "COMPLETED"
                for a in self._item_actions(item_id)
            )
            if settled_kind and kind == REFUND:
                raise RuleViolation("该责任项已完成退款，禁止重复退款（差额补发请用 TOPUP_REFUND）")
        return "ActionAssigned", {
            "action_id": action_id,
            "item_id": item_id,
            "kind": kind,
            "responsible_party": responsible_party,
            "deadline": parse_ts(deadline).isoformat(),
            "amount_cents": int(amount_cents),
        }

    def complete_action(self, *, action_id: str, result_ref: str | None = None,
                        at: datetime | None = None) -> tuple[str, dict]:
        self._require_open()
        action = self._action(action_id)
        if action.status == "COMPLETED":
            raise RuleViolation(f"动作已完成，不能重复完成: {action_id}")
        return "ActionCompleted", {
            "action_id": action_id,
            "item_id": action.item_id,
            "completed_at": (at or now_utc()).isoformat(),
            "result_ref": result_ref,
            "amount_cents": action.amount_cents,
        }

    def escalate_item(self, *, item_id: str, action_id: str, reason: str,
                      at: datetime, new_deadline: datetime | None = None,
                      to_party: str | None = None) -> tuple[str, dict]:
        """单个责任项超时升级；只影响该责任项，不牵连案件其他部分。

        要求对应未完成动作确实已过截止时间。可携带新的截止时间，
        表示该责任项的处理时钟被重置（旧截止仍保留在事件历史中）。
        """
        self._require_open()
        item = self._item(item_id)
        action = self._action(action_id)
        if action.item_id != item_id:
            raise RuleViolation("超时动作与责任项不匹配")
        if action.status == "COMPLETED":
            raise RuleViolation("动作已完成，不能因超时升级")
        if not is_overdue(action.deadline, at):
            raise RuleViolation(
                f"动作尚未超过截止时间 {action.deadline.isoformat()}，不能升级"
            )
        data = {
            "item_id": item_id,
            "action_id": action_id,
            "reason": reason,
            "escalated_at": parse_ts(at).isoformat(),
            "new_deadline": parse_ts(new_deadline).isoformat() if new_deadline else None,
            "to_party": to_party,
        }
        return "ItemEscalated", data

    def redact_personal_data(self, *, evidence_id: str, requested_by: str,
                             at: datetime | None = None) -> list[tuple[str, dict]]:
        """撤回/删除证据中的个人材料。

        证据正文被清空，但证据元数据（来源、类型、时间、哈希、所支撑判罚）
        仍然保留，案件可追溯性不受影响。重复撤回幂等，返回空列表。
        """
        evidence = self.evidences.get(evidence_id)
        if evidence is None:
            raise NotFound(f"证据不存在: {evidence_id}")
        if evidence.redacted:
            return []
        if not evidence.contains_personal_data:
            raise RuleViolation("该证据不含个人材料，无需撤回")
        return [("PersonalDataRedacted", {
            "evidence_id": evidence_id,
            "requested_by": requested_by,
            "redacted_at": (at or now_utc()).isoformat(),
        })]

    def close_case(self, *, at: datetime | None = None,
                   summary: str = "") -> tuple[str, dict]:
        """结案：必须等待**所有必需动作**完成。"""
        if self.status == CLOSED:
            raise CaseClosedError("案件已经结案")
        self._assert_closeable()
        return "CaseClosed", {
            "closed_at": (at or now_utc()).isoformat(),
            "summary": summary,
        }

    # -- 结算批次（财务） --------------------------------------------------

    def unsettled_fund_actions(self) -> list[Action_]:
        """已完成、已确认但尚未进入任何结算批次的资金动作。"""
        return [
            a for a in self.actions.values()
            if a.kind in FUND_ACTION_KINDS and a.status == "COMPLETED"
            and a.action_id not in self.batched_action_ids
        ]

    def create_settlement_batch(self, *, batch_id: str,
                                action_ids: list[str] | None = None,
                                at: datetime | None = None) -> tuple[str, dict]:
        """按已确认结果生成结算批次。

        - 只允许纳入**已完成**的资金动作，未完成/非资金动作一律拒绝；
        - 同一动作在案件全生命周期内只能进入一个批次（不重）；
        - 不强制一次结清，但 ``action_ids=None`` 时自动收纳全部待结算动作
          （不漏）；改判后的差额补发是新动作，自然进入后续批次。
        """
        if any(b["batch_id"] == batch_id for b in self.settlement_batches):
            raise RuleViolation(f"结算批次标识已存在: {batch_id}")
        targets = action_ids
        available = {a.action_id: a for a in self.unsettled_fund_actions()}
        if targets is None:
            chosen = list(available.values())
        else:
            if len(set(targets)) != len(targets):
                raise RuleViolation("批次内动作重复")
            missing_batched = [
                aid for aid in targets if aid in self.batched_action_ids
            ]
            if missing_batched:
                raise RuleViolation(
                    f"动作已在其他结算批次中，禁止重复结算: {missing_batched}"
                )
            chosen = []
            for aid in targets:
                action = self.actions.get(aid)
                if action is None:
                    raise NotFound(f"动作不存在: {aid}")
                if action.kind not in FUND_ACTION_KINDS:
                    raise RuleViolation(f"非资金动作不能进入结算: {aid}")
                if action.status != "COMPLETED":
                    raise RuleViolation(f"动作未完成，不能结算: {aid}")
                chosen.append(action)
        if not chosen:
            raise RuleViolation("没有可结算的已确认资金动作")
        chosen.sort(key=lambda a: (a.completed_at or now_utc(), a.action_id))
        lines = [
            {
                "action_id": a.action_id,
                "item_id": a.item_id,
                "kind": a.kind,
                "responsible_party": a.responsible_party,
                "amount_cents": a.amount_cents,
                "completed_at": (a.completed_at or now_utc()).isoformat(),
                "result_ref": a.result_ref,
            }
            for a in chosen
        ]
        return "SettlementBatchCreated", {
            "batch_id": batch_id,
            "created_at": (at or now_utc()).isoformat(),
            "lines": lines,
            "total_cents": sum(line["amount_cents"] for line in lines),
            "currency": self.currency,
            "case_status": self.status,
        }

    def confirm_settlement_batch(self, *, batch_id: str, confirmed_by: str,
                                 at: datetime | None = None) -> tuple[str, dict]:
        """复核确认批次：确认前重新比对每行金额与当前动作状态。"""
        batch = next((b for b in self.settlement_batches if b["batch_id"] == batch_id), None)
        if batch is None:
            raise NotFound(f"结算批次不存在: {batch_id}")
        if batch_id in self.confirmed_batches:
            raise RuleViolation(f"批次已确认，不能重复确认: {batch_id}")
        for line in batch["lines"]:
            action = self.actions[line["action_id"]]
            if action.status != "COMPLETED" or action.amount_cents != line["amount_cents"]:
                raise RuleViolation(
                    f"批次行 {line['action_id']} 与当前已确认结果不一致，不能确认"
                )
        return "SettlementBatchConfirmed", {
            "batch_id": batch_id,
            "confirmed_by": confirmed_by,
            "confirmed_at": (at or now_utc()).isoformat(),
            "total_cents": batch["total_cents"],
        }

    # -- 结案前置条件与不变量 ---------------------------------------------

    def _assert_closeable(self) -> None:
        if not self.items:
            raise RuleViolation("案件没有任何责任项，不能结案")
        undecided = [i.item_id for i in self.items.values()
                     if not i.decisions]
        if undecided:
            raise RuleViolation(f"责任项尚未判罚，不能结案: {undecided}")
        open_actions = [a.action_id for a in self.actions.values()
                        if a.status != "COMPLETED"]
        if open_actions:
            raise RuleViolation(f"存在未完成的必需动作，不能结案: {open_actions}")
        offer = self.current_offer
        if offer is not None and offer.chose_at is None:
            raise RuleViolation("消费者尚未作出选择，不能结案")
        self.check_invariants()

    def check_invariants(self) -> None:
        """金额守恒等核心不变量；事件回放后也可随时调用。"""
        for item in self.items.values():
            fund_total = sum(
                a.amount_cents for a in self._item_actions(item.item_id)
                if a.kind in FUND_ACTION_KINDS
            )
            if item.decisions:
                cap = item.decisions[-1].compensation_total_cents
                if fund_total > cap:
                    raise RuleViolation(
                        f"责任项 {item.item_id} 资金动作合计 {fund_total} "
                        f"超过判罚总额 {cap}"
                    )
            for decision in item.decisions:
                if sum(decision.shares_cents.values()) != decision.compensation_total_cents:
                    raise RuleViolation(
                        f"决策 {decision.decision_id} 分摊之和不等于补偿总额"
                    )

    def settled_cents_for_item(self, item_id: str) -> int:
        """消费者已实际收到的资金金额（改判下限）。"""
        return sum(
            a.amount_cents for a in self._item_actions(item_id)
            if a.kind in FUND_ACTION_KINDS and a.status == "COMPLETED"
        )

    def all_settled_cents(self) -> int:
        return sum(self.settled_cents_for_item(iid) for iid in self.items)

    # -- 视图辅助 ----------------------------------------------------------

    def _item_actions(self, item_id: str) -> list[Action_]:
        return [self.actions[aid] for aid in self._item(item_id).actions]

    def overdue_action_ids(self, at: datetime) -> list[str]:
        """给定时刻下已超时但未完成、且未被升级处理的动作。"""
        result = []
        for action in self.actions.values():
            if action.status != "COMPLETED" and not action.escalated and is_overdue(action.deadline, at):
                result.append(action.action_id)
        return result

    def required_actions_pending(self) -> list[str]:
        return [a.action_id for a in self.actions.values() if a.status != "COMPLETED"]

    # -- apply：事件 -> 状态 ------------------------------------------------

    def apply(self, event: dict[str, Any]) -> None:
        et = event["event_type"]
        data = event.get("data", {})
        seq = event["seq"]
        handler = getattr(self, f"_apply_{et.lower()}", None)
        if handler is None:
            raise RuleViolation(f"未知事件类型: {et}")
        handler(seq, data)
        self.events.append(event)

    def _apply_caseopened(self, seq: int, d: dict) -> None:
        self.status = OPEN

    def _apply_evidencesubmitted(self, seq: int, d: dict) -> None:
        key = d["dedup_key"]
        if key in self._dedup:  # 双保险：存储层已去重
            return
        evidence = Evidence(
            evidence_id=d["evidence_id"],
            source_party=d["source_party"],
            kind=d["kind"],
            dedup_key=key,
            submitted_at=parse_ts(d["submitted_at"]),
            contains_personal_data=d.get("contains_personal_data", False),
            content=d.get("content"),
            content_hash=d.get("content_hash"),
            material=d.get("material", False),
        )
        self.evidences[evidence.evidence_id] = evidence
        self._dedup.add(key)

    def _apply_liabilityitemadded(self, seq: int, d: dict) -> None:
        self.items[d["item_id"]] = LiabilityItem(
            item_id=d["item_id"], title=d["title"],
            linked_evidence=list(d.get("linked_evidence", [])),
        )

    def _apply_liabilityclaimed(self, seq: int, d: dict) -> None:
        item = self.items[d["item_id"]]
        if item.claimed_by is not None:
            # 重放遇到迟到的落选认领事件时不改变归属（正常存储层已拦截）。
            return
        item.claimed_by = d["party_id"]
        item.claimed_at = parse_ts(d["claimed_at"])

    def _apply_decisionrecorded(self, seq: int, d: dict) -> None:
        item = self.items[d["item_id"]]
        decision = Decision(
            event_seq=seq,
            decision_id=d["decision_id"],
            decided_by=d["decided_by"],
            reason=d["reason"],
            basis_evidence=list(d.get("basis_evidence", [])),
            shares_cents=dict(d["shares_cents"]),
            compensation_total_cents=d["compensation_total_cents"],
            supersedes_event_seq=d.get("supersedes_event_seq"),
            decided_at=parse_ts(d["decided_at"]),
        )
        item.decisions.append(decision)
        item.status = ITEM_DECIDED

    def _apply_consumeroffered(self, seq: int, d: dict) -> None:
        options = [
            OfferOption(
                option_id=o["option_id"], kind=o["kind"], label=o.get("label", o["kind"]),
                amount_cents=o.get("amount_cents", 0), benefit_ref=o.get("benefit_ref"),
            )
            for o in d["options"]
        ]
        self.offers.append(Offer(
            offer_id=d["offer_id"], options=options,
            issued_at=parse_ts(d["issued_at"]),
            valid_until=parse_ts(d["valid_until"]),
        ))

    def _apply_consumerchose(self, seq: int, d: dict) -> None:
        offer = self._find_offer(d["offer_id"])
        offer.chosen_option_id = d.get("option_id")
        offer.accepted = d["accepted"]
        offer.chose_at = parse_ts(d["chose_at"])

    def _apply_actionassigned(self, seq: int, d: dict) -> None:
        deadline = parse_ts(d["deadline"])
        action = Action_(
            action_id=d["action_id"], item_id=d["item_id"], kind=d["kind"],
            responsible_party=d["responsible_party"], deadline=deadline,
            amount_cents=d.get("amount_cents", 0),
        )
        action.deadline_history.append(deadline)
        self.actions[action.action_id] = action
        self.items[d["item_id"]].actions.append(action.action_id)

    def _apply_actioncompleted(self, seq: int, d: dict) -> None:
        action = self.actions[d["action_id"]]
        action.status = "COMPLETED"
        action.completed_at = parse_ts(d["completed_at"])
        action.result_ref = d.get("result_ref")

    def _apply_itemescalated(self, seq: int, d: dict) -> None:
        item = self.items[d["item_id"]]
        item.status = ITEM_ESCALATED
        item.escalation_level += 1
        action = self.actions[d["action_id"]]
        action.escalated = True
        if d.get("new_deadline"):
            new_deadline = parse_ts(d["new_deadline"])
            action.deadline_history.append(action.deadline)
            action.deadline = new_deadline
            action.escalated = False  # 时钟重置后等待下一次判定

    def _apply_personaldataredacted(self, seq: int, d: dict) -> None:
        evidence = self.evidences[d["evidence_id"]]
        evidence.content = None
        evidence.redacted = True
        evidence.redacted_at = parse_ts(d["redacted_at"])

    def _apply_caseclosed(self, seq: int, d: dict) -> None:
        self.status = CLOSED
        self.closed_at = parse_ts(d["closed_at"])

    def _apply_casereopened(self, seq: int, d: dict) -> None:
        self.status = REOPENED
        self.reopen_count += 1

    def _apply_settlementbatchcreated(self, seq: int, d: dict) -> None:
        self.settlement_batches.append(dict(d))
        self.batched_action_ids.update(line["action_id"] for line in d["lines"])

    def _apply_settlementbatchconfirmed(self, seq: int, d: dict) -> None:
        self.confirmed_batches.add(d["batch_id"])

    # -- 内部工具 ----------------------------------------------------------

    def _require_open(self) -> None:
        if self.status == CLOSED:
            raise CaseClosedError(
                "案件已结案；如有新的关键证据请先提交（material=True）触发重审"
            )

    def _item(self, item_id: str) -> LiabilityItem:
        try:
            return self.items[item_id]
        except KeyError:
            raise NotFound(f"责任项不存在: {item_id}") from None

    def _action(self, action_id: str) -> Action_:
        try:
            return self.actions[action_id]
        except KeyError:
            raise NotFound(f"动作不存在: {action_id}") from None

    def _find_offer(self, offer_id: str) -> Offer:
        for offer in reversed(self.offers):
            if offer.offer_id == offer_id:
                return offer
        raise NotFound(f"消费者选项不存在: {offer_id}")

    def _find_open_offer(self, offer_id: str) -> Offer:
        return self._find_offer(offer_id)
