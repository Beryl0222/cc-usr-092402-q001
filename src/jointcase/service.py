"""联合处置案件服务：命令执行、授权、并发重试与故障恢复。

服务层把"纯产生事件"的领域聚合与"追加日志"的存储连起来：

  重放案件 -> 校验授权 -> 执行命令得到事件 -> 持锁按期望序号追加；
  若序号被抢先（:class:`Conflict`），重新重放后重试命令。

对并发认领而言，落败方重试时命令本身会抛 :class:`AlreadyClaimed`，
从而得到稳定、明确的结果，而不是静默覆盖。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

from . import clock as clocks
from .domain import (
    ACTION_KINDS,
    CONSUMER,
    Case,
    CaseClosedError,
    JointCaseError,
    ORGANIZER,
    PLATFORM,
    REGULATOR,
    RESPONSIBLE_ROLES,
    RuleViolation,
    MERCHANT,
)
from .store import EventStore

ARBITER = "arbiter"
SUPERVISOR_ROLES = (ORGANIZER, ARBITER, REGULATOR, "system")
MAX_CAS_RETRIES = 25


class AuthorizationError(JointCaseError):
    """参与方未获授权执行该命令。"""


@dataclass
class PendingRecovery:
    """服务重启后扫描出的待办。"""

    case_id: str
    open_items: list[str]
    pending_actions: list[str]
    overdue_actions: list[str]
    awaiting_consumer_choice: bool
    case_status: str


class CaseService:
    def __init__(self, store: EventStore):
        self.store = store

    # -- 建案 --------------------------------------------------------------

    def create_case(self, *, case_id: str, actor: str, complaint_ref: str,
                    transaction_ref: str, consumer_id: str,
                    organizer_id: str = ORGANIZER, currency: str = "CNY",
                    at: datetime | None = None) -> Case:
        case = Case(
            case_id=case_id, complaint_ref=complaint_ref,
            transaction_ref=transaction_ref, consumer_id=consumer_id,
            organizer_id=organizer_id, currency=currency,
        )
        event = case.open_case(at=at)
        return self.store.init_case(case_id, event, actor=actor,
                                    occurred_at=event[1]["opened_at"])

    # -- 通用执行器 --------------------------------------------------------

    def _mutate(self, case_id: str, actor: str,
                command: Callable[[Case], list[tuple[str, dict]] | tuple[str, dict]],
                *, at: datetime | None = None) -> list[dict[str, Any]]:
        """重放 -> 命令 -> CAS 追加；冲突则重放重试。"""
        at = clocks.parse_ts(at) if at is not None else clocks.now_utc()
        for _ in range(MAX_CAS_RETRIES):
            case = self.store.load_case(case_id)
            result = command(case)
            if isinstance(result, tuple):
                pending = [result]
            else:
                pending = list(result)
            if not pending:
                return []  # 幂等无操作（重复证据/重复撤回）
            try:
                return self.store.append(
                    case_id, pending, actor=actor,
                    expected_seq=case.seq, occurred_at=at.isoformat(),
                )
            except _conflict_cls():
                continue
        raise JointCaseError(f"案件 {case_id} 并发竞争激烈，超过重试上限")

    # -- 证据 --------------------------------------------------------------

    def submit_evidence(self, case_id: str, *, actor: str, evidence_id: str,
                        source_party: str, kind: str,
                        at: datetime | None = None, **kwargs) -> list[dict]:
        # actor 可以代表来源方提交（如客服代录），但来源方必须是已知角色。
        if source_party not in RESPONSIBLE_ROLES + (CONSUMER,):
            raise AuthorizationError(f"未知证据来源方: {source_party}")
        return self._mutate(
            case_id, actor,
            lambda c: c.submit_evidence(
                evidence_id=evidence_id, source_party=source_party,
                kind=kind, submitted_at=at, **kwargs,
            ),
            at=at,
        )

    # -- 责任项 ------------------------------------------------------------

    def add_item(self, case_id: str, *, actor: str, item_id: str, title: str,
                 linked_evidence: list[str] | None = None) -> list[dict]:
        self._require_supervisor(actor)
        return self._mutate(
            case_id, actor,
            lambda c: [c.add_item(item_id=item_id, title=title,
                                  linked_evidence=linked_evidence)],
        )

    def claim_item(self, case_id: str, *, party_id: str, item_id: str) -> list[dict]:
        if party_id not in RESPONSIBLE_ROLES:
            raise AuthorizationError(f"参与方无资格认领: {party_id}")
        return self._mutate(
            case_id, party_id,
            lambda c: [c.claim_item(item_id=item_id, party_id=party_id)],
        )

    # -- 判罚 / 改判 -------------------------------------------------------

    def decide(self, case_id: str, *, actor: str, item_id: str, decision_id: str,
               reason: str, shares_cents: dict[str, int],
               basis_evidence: list[str] | None = None,
               supersedes_event_seq: int | None = None,
               at: datetime | None = None) -> list[dict]:
        self._require_decider(case_id, actor, item_id)
        return self._mutate(
            case_id, actor,
            lambda c: [c.decide(
                item_id=item_id, decision_id=decision_id, decided_by=actor,
                reason=reason, shares_cents=shares_cents,
                basis_evidence=basis_evidence,
                supersedes_event_seq=supersedes_event_seq,
            )],
            at=at,
        )

    def _require_decider(self, case_id: str, actor: str, item_id: str) -> None:
        if actor in SUPERVISOR_ROLES:
            return
        case = self.store.load_case(case_id)
        item = case._item(item_id)
        if item.claimed_by != actor:
            raise AuthorizationError(
                f"{actor} 未认领责任项 {item_id}，无权判罚；仲裁/主办方除外"
            )

    # -- 消费者选项 --------------------------------------------------------

    def offer_choices(self, case_id: str, *, actor: str, offer_id: str,
                      options: list[dict], valid_until: datetime) -> list[dict]:
        if actor not in (ORGANIZER, PLATFORM, ARBITER):
            raise AuthorizationError("只有主办方/权益平台/仲裁可向消费者提供选项")
        return self._mutate(
            case_id, actor,
            lambda c: [c.offer_choices(offer_id=offer_id, options=options,
                                       valid_until=valid_until)],
        )

    def consumer_choose(self, case_id: str, *, actor: str, offer_id: str,
                        option_id: str = "", accepted: bool = True,
                        at: datetime | None = None) -> list[dict]:
        case = self.store.load_case(case_id)
        # 消费者本人或客服代为确认均可，但必须带消费者身份。
        if actor != CONSUMER and not actor.startswith(f"{CONSUMER}:") \
                and actor not in ("agent", ORGANIZER):
            raise AuthorizationError("只有消费者本人或客服可记录消费者选择")
        return self._mutate(
            case_id, actor,
            lambda c: [c.consumer_choose(
                offer_id=offer_id, option_id=option_id, accepted=accepted,
            )],
            at=at,
        )

    # -- 动作与时钟 --------------------------------------------------------

    def assign_action(self, case_id: str, *, actor: str, action_id: str,
                      item_id: str, kind: str, responsible_party: str,
                      deadline: datetime, amount_cents: int = 0) -> list[dict]:
        if kind not in ACTION_KINDS:
            raise RuleViolation(f"未知动作类型: {kind}")
        if responsible_party not in RESPONSIBLE_ROLES:
            raise AuthorizationError(f"动作责任方必须是联防参与方: {responsible_party}")
        if actor not in SUPERVISOR_ROLES:
            case = self.store.load_case(case_id)
            item = case._item(item_id)
            if item.claimed_by not in (None, actor):
                raise AuthorizationError("只能为自己认领的责任项分派动作")
        return self._mutate(
            case_id, actor,
            lambda c: [c.assign_action(
                action_id=action_id, item_id=item_id, kind=kind,
                responsible_party=responsible_party, deadline=deadline,
                amount_cents=amount_cents,
            )],
        )

    def complete_action(self, case_id: str, *, actor: str, action_id: str,
                        result_ref: str | None = None,
                        at: datetime | None = None) -> list[dict]:
        case = self.store.load_case(case_id)
        action = case._action(action_id)
        if actor not in SUPERVISOR_ROLES and actor != "agent" and actor != action.responsible_party:
            raise AuthorizationError(
                f"动作 {action_id} 由 {action.responsible_party} 负责，{actor} 无权完成"
            )
        return self._mutate(
            case_id, actor,
            lambda c: [c.complete_action(action_id=action_id, result_ref=result_ref)],
            at=at,
        )

    def escalate_overdue(self, case_id: str, *, item_id: str, action_id: str,
                         actor: str = "system", reason: str = "监管时限超时",
                         at: datetime | None = None,
                         new_deadline: datetime | None = None,
                         to_party: str | None = None) -> list[dict]:
        """升级**单个**责任项：其他责任项与已完成补救不受影响。"""
        return self._mutate(
            case_id, actor,
            lambda c: [c.escalate_item(
                item_id=item_id, action_id=action_id, reason=reason,
                at=clocks.parse_ts(at) if at else clocks.now_utc(),
                new_deadline=new_deadline, to_party=to_party,
            )],
            at=at,
        )

    # -- 个人材料撤回 ------------------------------------------------------

    def redact_personal_data(self, case_id: str, *, actor: str, evidence_id: str,
                             at: datetime | None = None) -> list[dict]:
        # 消费者本人、主办方（受理撤回请求）或监管可触发。
        if actor not in (ORGANIZER, ARBITER, REGULATOR, "agent") \
                and not actor.startswith(f"{CONSUMER}:"):
            raise AuthorizationError("无权撤回个人材料")
        return self._mutate(
            case_id, actor,
            lambda c: c.redact_personal_data(evidence_id=evidence_id,
                                             requested_by=actor),
            at=at,
        )

    # -- 结案 --------------------------------------------------------------

    def close_case(self, case_id: str, *, actor: str, summary: str = "",
                   at: datetime | None = None) -> list[dict]:
        if actor not in SUPERVISOR_ROLES and actor != "agent":
            raise AuthorizationError("只有主办方/仲裁可结案")
        return self._mutate(
            case_id, actor,
            lambda c: [c.close_case(summary=summary)],
            at=at,
        )

    # -- 结算批次 ----------------------------------------------------------

    def create_settlement_batch(self, case_id: str, *, actor: str, batch_id: str,
                                action_ids: list[str] | None = None,
                                at: datetime | None = None) -> list[dict]:
        if actor not in ("finance", ORGANIZER, ARBITER, REGULATOR):
            raise AuthorizationError("只有财务角色可生成结算批次")
        return self._mutate(
            case_id, actor,
            lambda c: [c.create_settlement_batch(batch_id=batch_id,
                                                 action_ids=action_ids)],
            at=at,
        )

    def confirm_settlement_batch(self, case_id: str, *, actor: str,
                                 batch_id: str,
                                 at: datetime | None = None) -> list[dict]:
        if actor not in ("finance", ARBITER, REGULATOR):
            raise AuthorizationError("只有财务复核角色可确认批次")
        return self._mutate(
            case_id, actor,
            lambda c: [c.confirm_settlement_batch(batch_id=batch_id,
                                                  confirmed_by=actor)],
            at=at,
        )

    # -- 故障恢复：服务重启后的待办扫描 ------------------------------------

    def scan_pending(self, at: datetime | None = None) -> list[PendingRecovery]:
        """扫描全部案件日志，重建每个案件的待办（重启后调用）。

        只读、无副作用；调度器可据此对确已超时的责任项发起局部升级。
        """
        at = clocks.parse_ts(at) if at else clocks.now_utc()
        report: list[PendingRecovery] = []
        for case_id in self.store.list_cases():
            case = self.store.load_case(case_id)
            if case.status == "CLOSED":
                continue
            pending = case.required_actions_pending()
            overdue = case.overdue_action_ids(at)
            open_items = [
                i.item_id for i in case.items.values()
                if not i.actions or any(
                    case.actions[a].status != "COMPLETED" for a in i.actions
                )
            ]
            offer = case.current_offer
            awaiting = offer is not None and offer.chose_at is None
            report.append(PendingRecovery(
                case_id=case_id,
                open_items=open_items,
                pending_actions=pending,
                overdue_actions=overdue,
                awaiting_consumer_choice=awaiting,
                case_status=case.status,
            ))
        return report

    def recover_and_escalate(self, at: datetime | None = None,
                             reason: str = "服务重启后检测到监管时限超时"
                             ) -> dict[str, list[str]]:
        """恢复待办并把所有确已超时的动作按责任项逐个升级。

        已升级且已获得新截止时间的动作不会重复升级；
        已完成动作、其他责任项完全不受影响。
        """
        at = clocks.parse_ts(at) if at else clocks.now_utc()
        escalated: dict[str, list[str]] = {}
        for recovery in self.scan_pending(at):
            for action_id in recovery.overdue_actions:
                case = self.store.load_case(recovery.case_id)
                action = case._action(action_id)
                self.escalate_overdue(
                    recovery.case_id, item_id=action.item_id,
                    action_id=action_id, reason=reason, at=at,
                )
                escalated.setdefault(recovery.case_id, []).append(action_id)
        return escalated

    # -- 授权工具 ----------------------------------------------------------

    @staticmethod
    def _require_supervisor(actor: str) -> None:
        if actor not in SUPERVISOR_ROLES and actor != "agent":
            raise AuthorizationError(f"{actor} 无权执行该管理动作")


def _conflict_cls():
    from .domain import Conflict
    return Conflict
