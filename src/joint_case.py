"""跨城赛事投诉联合处置案件模型。

在 :mod:`src.event_consumer_guard` 的事件契约之上，把单笔投诉关联成一个
可追溯的联合处置案件（Case）。案件采用事件溯源：所有状态变更都是不可变
事件，命令（commitment）只做校验并追加事件，状态由事件重放得到，因此
人工改判、局部升级、迟到证据与故障恢复都能留下一致的时间线。

核心不变量：

* 证据按 ``evidence_id`` 幂等去重，迟到或重复提交不会产生第二条记录；
* 案件可拆成多个责任项（Item），结案要求所有必需责任项完成；
* 责任项只能由被指派方认领，认领是比较并追加（CAS），并发下仅一方成功；
* 超时只升级对应责任项，不自动退款、不影响其他责任项；
* 消费者接受的补救在履行前可被人工改判，但原决定与依据永久保留，
  且消费者必须重新接受新决定；履行后禁止改判，已接受的补救不会失效；
* 同一责任项同时只有一个生效决定，结算按“已完成且有生效补救”的责任项
  生成批次，做到不重不漏、金额守恒；
* 未授权参与方只能看到自己负责的责任项、自己提交的证据与本方的承诺。
"""

from __future__ import annotations

import functools
import json
import threading
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Callable, Iterable, Protocol

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

EVIDENCE_TRANSACTION_PROOF = "TRANSACTION_PROOF"
EVIDENCE_MERCHANT_COMMITMENT = "MERCHANT_COMMITMENT"
EVIDENCE_BENEFIT_REDEMPTION = "BENEFIT_REDEMPTION"
EVIDENCE_OTHER = "OTHER"
EVIDENCE_KINDS = frozenset(
    {
        EVIDENCE_TRANSACTION_PROOF,
        EVIDENCE_MERCHANT_COMMITMENT,
        EVIDENCE_BENEFIT_REDEMPTION,
        EVIDENCE_OTHER,
    }
)

ACTION_REFUND = "REFUND"
ACTION_COMPENSATE = "COMPENSATE"
ACTION_REISSUE_BENEFIT = "REISSUE_BENEFIT"
ACTION_OTHER = "ACTION"
REMEDY_ACTIONS = frozenset({ACTION_REFUND, ACTION_COMPENSATE, ACTION_REISSUE_BENEFIT})

ITEM_OPEN = "OPEN"
ITEM_CLAIMED = "CLAIMED"
ITEM_COMPLETED = "COMPLETED"
ITEM_CANCELLED = "CANCELLED"
ACTIVE_ITEM_STATES = frozenset({ITEM_OPEN, ITEM_CLAIMED})

DECISION_PROPOSED = "PROPOSED"
DECISION_ACCEPTED = "ACCEPTED"
DECISION_FULFILLED = "FULFILLED"
DECISION_SUPERSEDED = "SUPERSEDED"

EVIDENCE_ACTIVE = "ACTIVE"
EVIDENCE_WITHDRAWN = "WITHDRAWN"

BATCH_DRAFT = "DRAFT"
BATCH_CONFIRMED = "CONFIRMED"

ROLE_OPERATOR = "operator"      # 主办方平台客服 / 仲裁
ROLE_REGULATOR = "regulator"    # 监管
ROLE_CONSUMER = "consumer"      # 消费者
ROLE_PARTY = "party"            # 场馆商户、权益平台等被诉参与方
PRIVILEGED_ROLES = frozenset({ROLE_OPERATOR, ROLE_REGULATOR, ROLE_CONSUMER})

DEFAULT_GRACE = timedelta(hours=24)


class CaseError(ValueError):
    """案件命令违反领域规则。"""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


class DuplicateCommand(CaseError):
    """命令幂等键已处理过，直接拒绝，防止网络重试造成重复动作。"""

    def __init__(self, command_id: str):
        super().__init__("DUPLICATE_COMMAND", f"命令 {command_id} 已处理")
        self.command_id = command_id


# ---------------------------------------------------------------------------
# 时钟
# ---------------------------------------------------------------------------


class Clock(Protocol):
    def now(self) -> datetime: ...


@dataclass(frozen=True)
class FixedClock:
    """测试用固定时钟。"""

    instant: datetime

    def now(self) -> datetime:
        return self.instant


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


def to_dt(value: datetime | str | None) -> datetime | None:
    """把带偏移的 ISO 时间归一化为 UTC；朴素时间按 UTC 处理。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        moment = value
    else:
        moment = datetime.fromisoformat(value)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


def iso(moment: datetime) -> str:
    return to_dt(moment).isoformat()


# ---------------------------------------------------------------------------
# 事件存储
# ---------------------------------------------------------------------------


class InMemoryEventStore:
    """线程安全的内存事件流。"""

    def __init__(self) -> None:
        self._events: list[dict] = []
        self._lock = threading.Lock()

    def append(self, event: dict) -> None:
        with self._lock:
            self._events.append(event)

    def read_all(self) -> list[dict]:
        with self._lock:
            return list(self._events)


class FileEventStore:
    """JSONL 事件日志；服务重启后通过重放恢复全部待办。

    每行一个事件 JSON。追加在案件锁内调用并 flush，进程崩溃后
    已确认的命令不会丢失。
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def append(self, event: dict) -> None:
        line = json.dumps(event, ensure_ascii=False, sort_keys=True)
        with self._lock, self.path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
            fh.flush()

    def read_all(self) -> list[dict]:
        if not self.path.exists():
            return []
        with self._lock, self.path.open("r", encoding="utf-8") as fh:
            return [json.loads(line) for line in fh if line.strip()]


# ---------------------------------------------------------------------------
# 状态投影
# ---------------------------------------------------------------------------


def _money(value: Decimal | str | int) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _initial_state() -> dict:
    return {
        "case": None,
        "participants": {},          # party_id -> role
        "items": {},                 # item_id -> 责任项状态
        "evidence": {},              # evidence_id -> 证据状态
        "commitments": {},           # commitment_id -> 承诺
        "decisions": {},            # decision_id -> 决定（含历史版本）
        "batches": {},              # batch_id -> 批次
        "timeline": [],             # 全部事件（不可变）
        "command_ids": set(),
        "sequence": 0,
    }


def _apply(state: dict, event: dict) -> dict:
    """把单个事件折叠进状态（纯函数式改写）。"""
    etype = event["type"]
    state["sequence"] = event["seq"]
    state["timeline"].append(event)

    if etype == "CASE_OPENED":
        state["case"] = {
            "case_id": event["case_id"],
            "complaint_ref": event["complaint_ref"],
            "consumer_id": event["consumer_id"],
            "status": "OPEN",
            "opened_at": event["at"],
            "closed_at": None,
        }
        state["participants"] = dict(event["participants"])

    elif etype == "EVIDENCE_SUBMITTED":
        state["evidence"][event["evidence_id"]] = {
            "evidence_id": event["evidence_id"],
            "kind": event["kind"],
            "content_hash": event["content_hash"],
            "submitter": event["submitter"],
            "occurred_at": event["occurred_at"],
            "received_at": event["at"],
            "item_id": event.get("item_id"),
            "status": EVIDENCE_ACTIVE,
            "withdrawn_at": None,
        }

    elif etype == "EVIDENCE_WITHDRAWN":
        record = state["evidence"][event["evidence_id"]]
        record["status"] = EVIDENCE_WITHDRAWN
        record["withdrawn_at"] = event["at"]

    elif etype == "COMMITMENT_RECORDED":
        state["commitments"][event["commitment_id"]] = {
            "commitment_id": event["commitment_id"],
            "party": event["party"],
            "summary": event["summary"],
            "evidence_id": event.get("evidence_id"),
            "recorded_at": event["at"],
        }

    elif etype == "REDEMPTION_RECORDED":
        # 核销记录是特殊的证据，便于责任分摊时引用。
        state["evidence"][event["redemption_id"]] = {
            "evidence_id": event["redemption_id"],
            "kind": EVIDENCE_BENEFIT_REDEMPTION,
            "content_hash": event["content_hash"],
            "submitter": event["party"],
            "occurred_at": event["occurred_at"],
            "received_at": event["at"],
            "item_id": event.get("item_id"),
            "status": EVIDENCE_ACTIVE,
            "withdrawn_at": None,
            "benefit_ref": event["benefit_ref"],
        }

    elif etype == "ITEM_ADDED":
        state["items"][event["item_id"]] = {
            "item_id": event["item_id"],
            "title": event["title"],
            "responsible_party": event["responsible_party"],
            "action_type": event["action_type"],
            "required": event["required"],
            "due_at": event.get("due_at"),
            "status": ITEM_OPEN,
            "claimed_by": None,
            "claimed_at": None,
            "escalations": [],
            "decision_ids": [],
            "current_decision_id": None,
            "pending_override": None,
            "completed_at": None,
            "cancelled_at": None,
            "added_at": event["at"],
        }

    elif etype == "ITEM_CLAIMED":
        item = state["items"][event["item_id"]]
        item["status"] = ITEM_CLAIMED
        item["claimed_by"] = event["party"]
        item["claimed_at"] = event["at"]

    elif etype == "ITEM_ESCALATED":
        item = state["items"][event["item_id"]]
        item["escalations"].append(
            {"level": event["level"], "at": event["at"], "reason": event["reason"]}
        )

    elif etype == "DECISION_PROPOSED":
        decision = {
            "decision_id": event["decision_id"],
            "item_id": event["item_id"],
            "remedy_type": event["remedy_type"],
            "amount": Decimal(event["amount"]),
            "currency": event["currency"],
            "alternative": event.get("alternative", ""),
            "basis": list(event.get("basis", [])),
            "proposed_by": event["proposed_by"],
            "proposed_at": event["at"],
            "status": DECISION_PROPOSED,
            "accepted_at": None,
            "fulfilled_at": None,
            "fulfillment_ref": None,
            "superseded_at": None,
            "superseded_reason": None,
        }
        state["decisions"][event["decision_id"]] = decision
        item = state["items"][event["item_id"]]
        item["decision_ids"].append(event["decision_id"])
        if item["current_decision_id"] is None:
            item["current_decision_id"] = event["decision_id"]

    elif etype == "OVERRIDE_PROPOSED":
        item = state["items"][event["item_id"]]
        item["pending_override"] = {
            "old_decision_id": event["old_decision_id"],
            "new_decision_id": event["new_decision_id"],
            "reason": event["reason"],
            "by": event["by"],
            "at": event["at"],
        }

    elif etype == "OVERRIDE_WITHDRAWN":
        item = state["items"][event["item_id"]]
        pending = item["pending_override"]
        if pending is not None:
            rejected = state["decisions"][pending["new_decision_id"]]
            rejected["status"] = DECISION_SUPERSEDED
            rejected["superseded_at"] = event["at"]
            rejected["superseded_reason"] = "改判被撤回"
        item["pending_override"] = None

    elif etype == "DECISION_ACCEPTED":
        decision = state["decisions"][event["decision_id"]]
        decision["status"] = DECISION_ACCEPTED
        decision["accepted_at"] = event["at"]
        item = state["items"][event["item_id"]]
        pending = item["pending_override"]
        if pending and pending["new_decision_id"] == event["decision_id"]:
            old = state["decisions"][pending["old_decision_id"]]
            old["status"] = DECISION_SUPERSEDED
            old["superseded_at"] = event["at"]
            old["superseded_reason"] = pending["reason"]
            item["current_decision_id"] = event["decision_id"]
            item["pending_override"] = None

    elif etype == "REMEDY_FULFILLED":
        decision = state["decisions"][event["decision_id"]]
        decision["status"] = DECISION_FULFILLED
        decision["fulfilled_at"] = event["at"]
        decision["fulfillment_ref"] = event["fulfillment_ref"]
        item = state["items"][event["item_id"]]
        item["status"] = ITEM_COMPLETED
        item["completed_at"] = event["at"]

    elif etype == "ACTION_COMPLETED":
        item = state["items"][event["item_id"]]
        item["status"] = ITEM_COMPLETED
        item["completed_at"] = event["at"]
        item["completion_note"] = event["note"]

    elif etype == "ITEM_CANCELLED":
        item = state["items"][event["item_id"]]
        item["status"] = ITEM_CANCELLED
        item["cancelled_at"] = event["at"]
        item["cancel_reason"] = event["reason"]

    elif etype == "CASE_CLOSED":
        state["case"]["status"] = "CLOSED"
        state["case"]["closed_at"] = event["at"]

    elif etype == "SETTLEMENT_BATCH_CREATED":
        entries = [
            {**entry, "amount": Decimal(entry["amount"])}
            for entry in event["entries"]
        ]
        state["batches"][event["batch_id"]] = {
            "batch_id": event["batch_id"],
            "status": BATCH_DRAFT,
            "entries": entries,
            "created_at": event["at"],
            "confirmed_at": None,
        }

    elif etype == "SETTLEMENT_BATCH_CONFIRMED":
        batch = state["batches"][event["batch_id"]]
        batch["status"] = BATCH_CONFIRMED
        batch["confirmed_at"] = event["at"]

    else:  # pragma: no cover - 投影覆盖全部事件类型
        raise CaseError("UNKNOWN_EVENT", f"未知事件类型 {etype}")

    return state


# ---------------------------------------------------------------------------
# 案件聚合
# ---------------------------------------------------------------------------


class JointCase:
    """联合处置案件聚合根。

    命令方法完成校验并追加事件；所有方法在同一把案件锁内执行
    “读取状态—校验—追加—折叠”，因此多线程并发认领/重试是串行化的。
    """

    @staticmethod
    def _reject_duplicate(func: Callable) -> Callable:
        """命令入口先做幂等键检查，保证重试在业务校验之前被拒绝。

        锁内的 :meth:`_record` 仍会再查一次，作为并发下的权威防线。
        """

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            command_id = kwargs.get("command_id")
            if command_id is not None and command_id in self._state["command_ids"]:
                raise DuplicateCommand(command_id)
            return func(self, *args, **kwargs)

        return wrapper

    def __init__(
        self,
        store: InMemoryEventStore | FileEventStore,
        clock: Clock | None = None,
        state: dict | None = None,
        grace: timedelta = DEFAULT_GRACE,
    ):
        self._store = store
        self._clock = clock or SystemClock()
        self._state = state if state is not None else _initial_state()
        self._grace = grace
        self._lock = threading.RLock()

    # -- 基础设施 -----------------------------------------------------------

    @classmethod
    def open(
        cls,
        case_id: str,
        complaint_ref: str,
        consumer_id: str,
        participants: dict[str, str],
        store: InMemoryEventStore | FileEventStore | None = None,
        clock: Clock | None = None,
        command_id: str | None = None,
        now: datetime | str | None = None,
    ) -> "JointCase":
        """新建案件。``participants`` 为 party_id -> 角色（operator/party…）。"""
        case = cls(store or InMemoryEventStore(), clock)
        case._record(
            {
                "type": "CASE_OPENED",
                "case_id": case_id,
                "complaint_ref": complaint_ref,
                "consumer_id": consumer_id,
                "participants": dict(participants),
            },
            command_id,
            now,
        )
        return case

    @classmethod
    def rebuild(
        cls,
        store: InMemoryEventStore | FileEventStore,
        clock: Clock | None = None,
        grace: timedelta = DEFAULT_GRACE,
    ) -> "JointCase":
        """重放事件日志恢复案件——服务重启后的待办恢复入口。"""
        state = _initial_state()
        for raw in store.read_all():
            _apply(state, raw)
        state["command_ids"] = {
            event["command_id"]
            for event in state["timeline"]
            if event.get("command_id")
        }
        return cls(store, clock, state=state, grace=grace)

    @property
    def state(self) -> dict:
        return self._state

    @property
    def case_id(self) -> str | None:
        return None if self._state["case"] is None else self._state["case"]["case_id"]

    def _at(self, now: datetime | str | None) -> datetime:
        moment = to_dt(now) or to_dt(self._clock.now())
        assert moment is not None
        return moment

    def _record(
        self,
        event: dict,
        command_id: str | None,
        now: datetime | str | None,
    ) -> dict:
        """追加事件的唯一入口，承担幂等键、序号与加锁。"""
        with self._lock:
            if command_id is not None:
                if command_id in self._state["command_ids"]:
                    raise DuplicateCommand(command_id)
                self._state["command_ids"].add(command_id)
            event = {
                **event,
                "seq": self._state["sequence"] + 1,
                "at": iso(self._at(now)),
                "command_id": command_id,
            }
            self._store.append(event)
            _apply(self._state, event)
            return event

    def _require_open_case(self) -> dict:
        case = self._state["case"]
        if case is None:
            raise CaseError("CASE_NOT_OPENED", "案件尚未创建")
        return case

    def _item(self, item_id: str) -> dict:
        item = self._state["items"].get(item_id)
        if item is None:
            raise CaseError("ITEM_NOT_FOUND", f"责任项 {item_id} 不存在")
        return item

    def _decision(self, decision_id: str) -> dict:
        decision = self._state["decisions"].get(decision_id)
        if decision is None:
            raise CaseError("DECISION_NOT_FOUND", f"决定 {decision_id} 不存在")
        return decision

    def _require_role(self, party: str, *roles: str) -> str:
        actual = self._state["participants"].get(party)
        if actual is None:
            raise CaseError("UNAUTHORIZED_PARTY", f"{party} 不是案件参与方")
        if roles and actual not in roles:
            raise CaseError("FORBIDDEN_ROLE", f"{party} 角色 {actual} 无权执行该动作")
        return actual

    # -- 证据 / 承诺 / 核销 --------------------------------------------------

    @_reject_duplicate
    def submit_evidence(
        self,
        evidence_id: str,
        kind: str,
        content_hash: str,
        submitter: str,
        occurred_at: datetime | str,
        item_id: str | None = None,
        command_id: str | None = None,
        now: datetime | str | None = None,
    ) -> dict:
        """提交交易凭证等证据。

        按 ``evidence_id`` 幂等：迟到、重复提交返回已有事件，不产生新记录。
        """
        with self._lock:
            self._require_open_case()
            if kind not in EVIDENCE_KINDS:
                raise CaseError("BAD_EVIDENCE_KIND", f"证据类型 {kind} 不合法")
            if submitter != self._state["case"]["consumer_id"]:
                self._require_role(submitter, ROLE_PARTY, ROLE_OPERATOR, ROLE_REGULATOR)
            if item_id is not None:
                self._item(item_id)
            existing = self._state["evidence"].get(evidence_id)
            if existing is not None:
                # 相同证据迟到或重复：幂等返回，时间线不重复。
                return next(
                    e for e in self._state["timeline"]
                    if e["type"] == "EVIDENCE_SUBMITTED" and e["evidence_id"] == evidence_id
                )
            return self._record(
                {
                    "type": "EVIDENCE_SUBMITTED",
                    "evidence_id": evidence_id,
                    "kind": kind,
                    "content_hash": content_hash,
                    "submitter": submitter,
                    "occurred_at": iso(to_dt(occurred_at)),
                    "item_id": item_id,
                },
                command_id,
                now,
            )

    @_reject_duplicate
    def withdraw_evidence(
        self,
        evidence_id: str,
        by: str,
        command_id: str | None = None,
        now: datetime | str | None = None,
    ) -> dict:
        """撤回个人材料：只留下撤回墓碑，内容对非授权视图不可见。"""
        with self._lock:
            record = self._state["evidence"].get(evidence_id)
            if record is None:
                raise CaseError("EVIDENCE_NOT_FOUND", f"证据 {evidence_id} 不存在")
            if record["submitter"] != by and self._state["participants"].get(by) != ROLE_OPERATOR:
                raise CaseError("FORBIDDEN_ROLE", "只有提交者或客服可撤回材料")
            if record["status"] == EVIDENCE_WITHDRAWN:
                raise CaseError("ALREADY_WITHDRAWN", "材料已撤回")
            return self._record(
                {"type": "EVIDENCE_WITHDRAWN", "evidence_id": evidence_id, "by": by},
                command_id,
                now,
            )

    @_reject_duplicate
    def record_commitment(
        self,
        commitment_id: str,
        party: str,
        summary: str,
        evidence_id: str | None = None,
        command_id: str | None = None,
        now: datetime | str | None = None,
    ) -> dict:
        """登记商户/平台对消费者作出过的承诺，作为责任认定依据。"""
        with self._lock:
            self._require_open_case()
            self._require_role(party, ROLE_PARTY, ROLE_OPERATOR)
            if evidence_id is not None and evidence_id not in self._state["evidence"]:
                raise CaseError("EVIDENCE_NOT_FOUND", f"证据 {evidence_id} 不存在")
            if commitment_id in self._state["commitments"]:
                raise CaseError("DUPLICATE_COMMITMENT", f"承诺 {commitment_id} 已登记")
            return self._record(
                {
                    "type": "COMMITMENT_RECORDED",
                    "commitment_id": commitment_id,
                    "party": party,
                    "summary": summary,
                    "evidence_id": evidence_id,
                },
                command_id,
                now,
            )

    @_reject_duplicate
    def record_redemption(
        self,
        redemption_id: str,
        benefit_ref: str,
        party: str,
        content_hash: str,
        occurred_at: datetime | str,
        item_id: str | None = None,
        command_id: str | None = None,
        now: datetime | str | None = None,
    ) -> dict:
        """登记权益核销记录（幂等）。"""
        with self._lock:
            self._require_open_case()
            self._require_role(party, ROLE_PARTY, ROLE_OPERATOR)
            if redemption_id in self._state["evidence"]:
                return next(
                    e for e in self._state["timeline"]
                    if e["type"] == "REDEMPTION_RECORDED" and e["redemption_id"] == redemption_id
                )
            return self._record(
                {
                    "type": "REDEMPTION_RECORDED",
                    "redemption_id": redemption_id,
                    "benefit_ref": benefit_ref,
                    "party": party,
                    "content_hash": content_hash,
                    "occurred_at": iso(to_dt(occurred_at)),
                    "item_id": item_id,
                },
                command_id,
                now,
            )

    # -- 责任项 -------------------------------------------------------------

    @_reject_duplicate
    def add_item(
        self,
        item_id: str,
        title: str,
        responsible_party: str,
        action_type: str,
        due_at: datetime | str | None = None,
        required: bool = True,
        command_id: str | None = None,
        now: datetime | str | None = None,
    ) -> dict:
        """把案件拆成责任项；``due_at`` 可为任意时区，内部按 UTC 比较。"""
        with self._lock:
            self._require_open_case()
            self._require_role(responsible_party, ROLE_PARTY, ROLE_OPERATOR)
            if item_id in self._state["items"]:
                raise CaseError("DUPLICATE_ITEM", f"责任项 {item_id} 已存在")
            if action_type not in REMEDY_ACTIONS | {ACTION_OTHER}:
                raise CaseError("BAD_ACTION_TYPE", f"动作类型 {action_type} 不合法")
            return self._record(
                {
                    "type": "ITEM_ADDED",
                    "item_id": item_id,
                    "title": title,
                    "responsible_party": responsible_party,
                    "action_type": action_type,
                    "required": required,
                    "due_at": iso(to_dt(due_at)) if due_at is not None else None,
                },
                command_id,
                now,
            )

    @_reject_duplicate
    def claim_item(
        self,
        item_id: str,
        party: str,
        command_id: str | None = None,
        now: datetime | str | None = None,
    ) -> dict:
        """认领责任项。仅被指派方可认领；并发下只有一方成功。"""
        with self._lock:
            item = self._item(item_id)
            self._require_role(party, ROLE_PARTY, ROLE_OPERATOR)
            if item["responsible_party"] != party:
                raise CaseError("NOT_RESPONSIBLE", f"{party} 不是责任项 {item_id} 的负责方")
            if item["status"] != ITEM_OPEN:
                raise CaseError(
                    "ITEM_NOT_CLAIMABLE",
                    f"责任项 {item_id} 状态为 {item['status']}，无法认领",
                )
            return self._record(
                {"type": "ITEM_CLAIMED", "item_id": item_id, "party": party},
                command_id,
                now,
            )

    def sweep(self, now: datetime | str | None = None) -> list[dict]:
        """扫描超时责任项并升级。

        * 超过 ``due_at``：一级升级（L1）；
        * 超过 ``due_at + grace``：二级升级（L2）。

        每个级别只产生一次升级事件，因此重复扫描/重启后扫描都是幂等的；
        超时仅升级对应责任项，不触发退款，也不改变其他责任项。
        """
        with self._lock:
            moment = self._at(now)
            fired: list[dict] = []
            for item in list(self._state["items"].values()):
                if item["status"] not in ACTIVE_ITEM_STATES or not item["due_at"]:
                    continue
                due = to_dt(item["due_at"])
                level = len(item["escalations"])
                if level == 0 and moment >= due:
                    fired.append(
                        self._record(
                            {
                                "type": "ITEM_ESCALATED",
                                "item_id": item["item_id"],
                                "level": 1,
                                "reason": "超过处理时限",
                            },
                            None,
                            moment,
                        )
                    )
                elif level == 1 and moment >= due + self._grace:
                    fired.append(
                        self._record(
                            {
                                "type": "ITEM_ESCALATED",
                                "item_id": item["item_id"],
                                "level": 2,
                                "reason": "超过宽限期仍未完成",
                            },
                            None,
                            moment,
                        )
                    )
            return fired

    def due_items(self, now: datetime | str | None = None) -> list[dict]:
        """当前已到时限但未完成的责任项——重启后恢复待办的查询入口。"""
        moment = self._at(now)
        result = []
        for item in self._state["items"].values():
            if item["status"] in ACTIVE_ITEM_STATES and item["due_at"]:
                if moment >= to_dt(item["due_at"]):
                    result.append(item)
        return result

    # -- 决定 / 消费者选择 / 改判 -------------------------------------------

    @_reject_duplicate
    def propose_decision(
        self,
        item_id: str,
        decision_id: str,
        remedy_type: str,
        amount: Decimal | str,
        proposed_by: str,
        currency: str = "CNY",
        alternative: str = "",
        basis: Iterable[str] = (),
        command_id: str | None = None,
        now: datetime | str | None = None,
    ) -> dict:
        """就责任项提出处置决定（部分退款 / 替代权益等）并附依据证据。"""
        with self._lock:
            item = self._item(item_id)
            self._require_role(proposed_by, ROLE_PARTY, ROLE_OPERATOR)
            if item["status"] != ITEM_CLAIMED:
                raise CaseError("ITEM_NOT_CLAIMED", "责任项认领后才能提出决定")
            if decision_id in self._state["decisions"]:
                raise CaseError("DUPLICATE_DECISION", f"决定 {decision_id} 已存在")
            amount_dec = _money(amount)
            if amount_dec < 0:
                raise CaseError("BAD_AMOUNT", "金额不能为负")
            basis = list(basis)
            for evidence_id in basis:
                if evidence_id not in self._state["evidence"]:
                    raise CaseError("EVIDENCE_NOT_FOUND", f"依据 {evidence_id} 不存在")
            current_id = item["current_decision_id"]
            if current_id is not None:
                current = self._state["decisions"][current_id]
                if current["status"] == DECISION_PROPOSED:
                    raise CaseError("DECISION_PENDING", "已有待消费者接受的决定")
                raise CaseError(
                    "DECISION_EXISTS",
                    "该责任项已有生效决定，人工改判请使用 propose_override",
                )
            return self._record(
                {
                    "type": "DECISION_PROPOSED",
                    "decision_id": decision_id,
                    "item_id": item_id,
                    "remedy_type": remedy_type,
                    "amount": str(amount_dec),
                    "currency": currency,
                    "alternative": alternative,
                    "basis": basis,
                    "proposed_by": proposed_by,
                },
                command_id,
                now,
            )

    @_reject_duplicate
    def accept_decision(
        self,
        item_id: str,
        decision_id: str,
        consumer: str,
        command_id: str | None = None,
        now: datetime | str | None = None,
    ) -> dict:
        """消费者接受决定。接受即锁定为该责任项的当前生效补救。"""
        with self._lock:
            item = self._item(item_id)
            decision = self._decision(decision_id)
            if decision["item_id"] != item_id:
                raise CaseError("DECISION_MISMATCH", "决定不属于该责任项")
            if consumer != self._state["case"]["consumer_id"]:
                raise CaseError("FORBIDDEN_ROLE", "只有消费者本人可以接受决定")
            pending = item["pending_override"]
            if pending is not None and decision_id != pending["new_decision_id"]:
                raise CaseError(
                    "OVERRIDE_PENDING",
                    "该责任项有等待消费者确认的改判决定，请接受新决定或由客服撤回改判",
                )
            if decision["status"] != DECISION_PROPOSED:
                raise CaseError(
                    "DECISION_NOT_OPEN",
                    f"决定状态为 {decision['status']}，无法接受",
                )
            return self._record(
                {
                    "type": "DECISION_ACCEPTED",
                    "decision_id": decision_id,
                    "item_id": item_id,
                    "consumer": consumer,
                },
                command_id,
                now,
            )

    @_reject_duplicate
    def propose_override(
        self,
        item_id: str,
        new_decision_id: str,
        remedy_type: str,
        amount: Decimal | str,
        reason: str,
        by: str,
        currency: str = "CNY",
        alternative: str = "",
        basis: Iterable[str] = (),
        command_id: str | None = None,
        now: datetime | str | None = None,
    ) -> list[dict]:
        """人工改判：在已接受的补救上提出新版本。

        原决定保持 ACCEPTED 直到消费者接受新版本，因此改判不会让已接受
        的补救立即失效；已履行（FULFILLED）的决定禁止改判，杜绝重复退款。
        原决定、依据、改判理由与改判人全部保留在时间线上。
        """
        with self._lock:
            item = self._item(item_id)
            self._require_role(by, ROLE_OPERATOR)
            old_id = item["current_decision_id"]
            if old_id is None:
                raise CaseError("NO_DECISION", "该责任项尚无决定，应直接提出决定")
            old = self._decision(old_id)
            if old["status"] == DECISION_FULFILLED:
                raise CaseError("REMEDY_LOCKED", "补救已履行，不能改判或重复退款")
            if old["status"] != DECISION_ACCEPTED:
                raise CaseError("DECISION_NOT_ACCEPTED", "只能对已接受的决定改判")
            if item["pending_override"] is not None:
                raise CaseError("OVERRIDE_PENDING", "已有改判等待消费者确认")
            amount_dec = _money(amount)
            if amount_dec < 0:
                raise CaseError("BAD_AMOUNT", "金额不能为负")
            basis = list(basis)
            proposed = self._record(
                {
                    "type": "DECISION_PROPOSED",
                    "decision_id": new_decision_id,
                    "item_id": item_id,
                    "remedy_type": remedy_type,
                    "amount": str(amount_dec),
                    "currency": currency,
                    "alternative": alternative,
                    "basis": basis,
                    "proposed_by": by,
                },
                command_id,
                now,
            )
            override = self._record(
                {
                    "type": "OVERRIDE_PROPOSED",
                    "item_id": item_id,
                    "old_decision_id": old_id,
                    "new_decision_id": new_decision_id,
                    "reason": reason,
                    "by": by,
                },
                None,
                now,
            )
            return [proposed, override]

    @_reject_duplicate
    def withdraw_override(
        self,
        item_id: str,
        by: str,
        command_id: str | None = None,
        now: datetime | str | None = None,
    ) -> dict:
        """撤回改判提案，原接受决定继续生效。"""
        with self._lock:
            item = self._item(item_id)
            self._require_role(by, ROLE_OPERATOR)
            if item["pending_override"] is None:
                raise CaseError("NO_OVERRIDE", "该责任项没有待确认的改判")
            return self._record(
                {"type": "OVERRIDE_WITHDRAWN", "item_id": item_id, "by": by},
                command_id,
                now,
            )

    @_reject_duplicate
    def fulfill_remedy(
        self,
        item_id: str,
        fulfillment_ref: str,
        by: str,
        command_id: str | None = None,
        now: datetime | str | None = None,
    ) -> dict:
        """履行补救（退款到账 / 替代权益发放）。履行后责任项完成且锁定。"""
        with self._lock:
            item = self._item(item_id)
            self._require_role(by, ROLE_PARTY, ROLE_OPERATOR)
            decision_id = item["current_decision_id"]
            if decision_id is None:
                raise CaseError("NO_DECISION", "责任项尚无决定")
            decision = self._decision(decision_id)
            if decision["status"] == DECISION_FULFILLED:
                raise CaseError("ALREADY_FULFILLED", "不能重复履行退款/补救")
            if decision["status"] != DECISION_ACCEPTED:
                raise CaseError("DECISION_NOT_ACCEPTED", "决定须先经消费者接受")
            if item["pending_override"] is not None:
                raise CaseError("OVERRIDE_PENDING", "改判待消费者确认，不能履行旧决定")
            if item["action_type"] == ACTION_OTHER:
                raise CaseError("BAD_ACTION_TYPE", "非补救责任项请使用 complete_action")
            return self._record(
                {
                    "type": "REMEDY_FULFILLED",
                    "item_id": item_id,
                    "decision_id": decision_id,
                    "fulfillment_ref": fulfillment_ref,
                    "by": by,
                },
                command_id,
                now,
            )

    @_reject_duplicate
    def complete_action(
        self,
        item_id: str,
        note: str,
        by: str,
        command_id: str | None = None,
        now: datetime | str | None = None,
    ) -> dict:
        """完成非金钱补救责任项（如更正承诺、出具说明）。"""
        with self._lock:
            item = self._item(item_id)
            self._require_role(by, ROLE_PARTY, ROLE_OPERATOR)
            if item["action_type"] != ACTION_OTHER:
                raise CaseError("BAD_ACTION_TYPE", "补救类责任项请使用 fulfill_remedy")
            if item["status"] not in ACTIVE_ITEM_STATES:
                raise CaseError("ITEM_NOT_ACTIVE", "责任项不在处理中")
            return self._record(
                {"type": "ACTION_COMPLETED", "item_id": item_id, "note": note, "by": by},
                command_id,
                now,
            )

    @_reject_duplicate
    def cancel_item(
        self,
        item_id: str,
        reason: str,
        by: str,
        command_id: str | None = None,
        now: datetime | str | None = None,
    ) -> dict:
        """取消非必需责任项；必需责任项不可取消，必须走完或改判。"""
        with self._lock:
            item = self._item(item_id)
            self._require_role(by, ROLE_OPERATOR)
            if item["required"]:
                raise CaseError("ITEM_REQUIRED", "必需责任项不能取消")
            if item["status"] not in ACTIVE_ITEM_STATES:
                raise CaseError("ITEM_NOT_ACTIVE", "责任项不在处理中")
            return self._record(
                {"type": "ITEM_CANCELLED", "item_id": item_id, "reason": reason, "by": by},
                command_id,
                now,
            )

    # -- 结案 / 结算 ---------------------------------------------------------

    @_reject_duplicate
    def close_case(
        self,
        command_id: str | None = None,
        now: datetime | str | None = None,
    ) -> dict:
        """结案：所有必需责任项必须完成，且没有挂起的处理中责任项。"""
        with self._lock:
            self._require_open_case()
            required_open = [
                i["item_id"]
                for i in self._state["items"].values()
                if i["required"] and i["status"] != ITEM_COMPLETED
            ]
            if required_open:
                raise CaseError(
                    "REQUIRED_ITEMS_OPEN",
                    f"必需责任项尚未完成：{', '.join(required_open)}",
                )
            active = [
                i["item_id"]
                for i in self._state["items"].values()
                if i["status"] in ACTIVE_ITEM_STATES
            ]
            if active:
                raise CaseError(
                    "ITEMS_STILL_ACTIVE",
                    f"仍有处理中的非必需责任项：{', '.join(active)}",
                )
            return self._record({"type": "CASE_CLOSED"}, command_id, now)

    def _batchable_items(self) -> list[dict]:
        items = []
        for item in self._state["items"].values():
            if item["status"] != ITEM_COMPLETED or item["action_type"] == ACTION_OTHER:
                continue
            decision = self._state["decisions"][item["current_decision_id"]]
            if decision["status"] == DECISION_FULFILLED and decision["amount"] > 0:
                items.append(item)
        return items

    @_reject_duplicate
    def create_settlement_batch(
        self,
        batch_id: str,
        by: str,
        command_id: str | None = None,
        now: datetime | str | None = None,
    ) -> dict:
        """按已确认结果生成结算批次。

        只纳入“已完成、已履行、金额为正且未进过任何批次”的责任项，
        因此同一笔补救不会跨批次重复结算；遗漏的新完成项在下次生成时补齐。
        """
        with self._lock:
            self._require_role(by, ROLE_OPERATOR)
            if batch_id in self._state["batches"]:
                raise CaseError("DUPLICATE_BATCH", f"批次 {batch_id} 已存在")
            already_batched = {
                entry["item_id"]
                for batch in self._state["batches"].values()
                for entry in batch["entries"]
            }
            pending = [i for i in self._batchable_items() if i["item_id"] not in already_batched]
            if not pending:
                raise CaseError("NOTHING_TO_SETTLE", "没有待结算的已完成补救")
            entries = []
            for item in pending:
                decision = self._state["decisions"][item["current_decision_id"]]
                entries.append(
                    {
                        "item_id": item["item_id"],
                        "payer_party": item["responsible_party"],
                        "payee_consumer": self._state["case"]["consumer_id"],
                        "decision_id": decision["decision_id"],
                        "remedy_type": decision["remedy_type"],
                        "currency": decision["currency"],
                        "amount": str(decision["amount"]),
                        "fulfillment_ref": decision["fulfillment_ref"],
                    }
                )
            return self._record(
                {
                    "type": "SETTLEMENT_BATCH_CREATED",
                    "batch_id": batch_id,
                    "by": by,
                    "entries": entries,
                },
                command_id,
                now,
            )

    @_reject_duplicate
    def confirm_batch(
        self,
        batch_id: str,
        by: str,
        command_id: str | None = None,
        now: datetime | str | None = None,
    ) -> dict:
        """确认批次（财务出账）。确认后批次冻结，条目不可再被结算。"""
        with self._lock:
            batch = self._state["batches"].get(batch_id)
            if batch is None:
                raise CaseError("BATCH_NOT_FOUND", f"批次 {batch_id} 不存在")
            self._require_role(by, ROLE_OPERATOR)
            if batch["status"] == BATCH_CONFIRMED:
                raise CaseError("BATCH_ALREADY_CONFIRMED", "批次已确认")
            return self._record(
                {"type": "SETTLEMENT_BATCH_CONFIRMED", "batch_id": batch_id, "by": by},
                command_id,
                now,
            )

    # -- 审计 / 查询 ---------------------------------------------------------

    def settlement_audit(self) -> dict:
        """金额守恒与“不重不漏”审计。

        ``expected``：已完成且已履行的有效补救金额（按币种汇总）；
        ``batched``：所有批次（含草稿）覆盖的金额；
        ``unbatched``：已完成但尚未进批次的金额；
        要求 expected == batched + unbatched，且每个责任项在批次中至多一条。
        """
        expected: dict[str, Decimal] = {}
        batched: dict[str, Decimal] = {}
        issues: list[str] = []

        effective: dict[str, Decimal] = {}
        for item in self._batchable_items():
            decision = self._state["decisions"][item["current_decision_id"]]
            effective[item["item_id"]] = decision["amount"]
            expected[decision["currency"]] = expected.get(decision["currency"], Decimal()) + decision["amount"]

        seen: set[str] = set()
        for batch in self._state["batches"].values():
            for entry in batch["entries"]:
                if entry["item_id"] in seen:
                    issues.append(f"责任项 {entry['item_id']} 在多个批次条目中重复出现")
                seen.add(entry["item_id"])
                if entry["item_id"] not in effective:
                    issues.append(f"批次 {batch['batch_id']} 纳入了无有效履行结果的责任项 {entry['item_id']}")
                elif effective[entry["item_id"]] != entry["amount"]:
                    issues.append(
                        f"责任项 {entry['item_id']} 批次金额 {entry['amount']} "
                        f"与生效决定金额 {effective[entry['item_id']]} 不一致"
                    )
                batched[entry["currency"]] = batched.get(entry["currency"], Decimal()) + entry["amount"]

        unbatched: dict[str, Decimal] = {}
        for item_id, amount in effective.items():
            if item_id not in seen:
                item = self._state["items"][item_id]
                currency = self._state["decisions"][item["current_decision_id"]]["currency"]
                unbatched[currency] = unbatched.get(currency, Decimal()) + amount

        for currency in set(expected) | set(batched) | set(unbatched):
            delta = expected.get(currency, Decimal()) - batched.get(currency, Decimal()) - unbatched.get(currency, Decimal())
            if delta != 0:
                issues.append(f"币种 {currency} 金额不守恒：差额 {delta}")
        return {
            "ok": not issues,
            "issues": issues,
            "expected": dict(expected),
            "batched": dict(batched),
            "unbatched": dict(unbatched),
        }

    def explain(self) -> dict:
        """给客服的案件解释：当前责任、下一步与金额汇总。"""
        case = self._require_open_case()
        items = []
        for item in self._state["items"].values():
            decision = (
                self._state["decisions"].get(item["current_decision_id"])
                if item["current_decision_id"]
                else None
            )
            items.append(
                {
                    "item_id": item["item_id"],
                    "title": item["title"],
                    "responsible_party": item["responsible_party"],
                    "status": item["status"],
                    "escalation_level": len(item["escalations"]),
                    "due_at": item["due_at"],
                    "next_step": self._next_step(item, decision),
                    "amount": None if decision is None else str(decision["amount"]),
                    "currency": None if decision is None else decision["currency"],
                    "remedy_type": None if decision is None else decision["remedy_type"],
                }
            )
        audit = self.settlement_audit()
        return {
            "case_id": case["case_id"],
            "status": case["status"],
            "items": items,
            "expected_total": {k: str(v) for k, v in audit["expected"].items()},
            "batched_total": {k: str(v) for k, v in audit["batched"].items()},
            "unbatched_total": {k: str(v) for k, v in audit["unbatched"].items()},
            "money_conservation_ok": audit["ok"],
        }

    def _next_step(self, item: dict, decision: dict | None) -> str:
        if item["status"] == ITEM_COMPLETED:
            return "已完成，等待结算" if item["action_type"] != ACTION_OTHER else "已完成"
        if item["status"] == ITEM_CANCELLED:
            return "已取消"
        if item["status"] == ITEM_OPEN:
            if item["escalations"]:
                return f"已升级 L{len(item['escalations'])}，等待 {item['responsible_party']} 认领"
            return f"等待 {item['responsible_party']} 在时限内认领"
        if item["pending_override"] is not None:
            return "改判待消费者确认"
        if decision is None:
            return f"等待 {item['claimed_by']} 提出处置决定"
        if decision["status"] == DECISION_PROPOSED:
            return "等待消费者接受决定"
        if decision["status"] == DECISION_ACCEPTED:
            return f"等待 {item['responsible_party']} 履行补救"
        return "处理中"

    # -- 可见性 --------------------------------------------------------------

    def _is_privileged(self, viewer: str | None) -> bool:
        if viewer is None:
            return True
        if self._state["participants"].get(viewer) in PRIVILEGED_ROLES:
            return True
        case = self._state["case"]
        return case is not None and viewer == case["consumer_id"]

    def _evidence_visible(self, evidence_id: str, viewer: str | None) -> bool:
        if self._is_privileged(viewer):
            return True
        record = self._state["evidence"].get(evidence_id)
        if record is None:
            return False
        if record["submitter"] == viewer:
            return True
        linked = record["item_id"]
        if linked and self._state["items"][linked]["responsible_party"] == viewer:
            return True
        # 责任项决定所引用的依据，对该责任项负责方可见，以便其答辩。
        for item in self._state["items"].values():
            if item["responsible_party"] != viewer:
                continue
            for decision_id in item["decision_ids"]:
                if evidence_id in self._state["decisions"][decision_id]["basis"]:
                    return True
        return False

    def _item_visible(self, item: dict, viewer: str | None) -> bool:
        if self._is_privileged(viewer):
            return True
        return item["responsible_party"] == viewer

    def view_for(self, viewer: str | None = None) -> dict:
        """按参与方授权返回裁剪后的案件视图。

        未授权参与方只能看到自己负责的责任项、自己提交/被引为依据的证据、
        本方作出的承诺；撤回中的个人材料只显示墓碑，不显示内容哈希。
        """
        if viewer is not None and self._state["case"] is not None:
            if viewer != self._state["case"]["consumer_id"] and viewer not in self._state["participants"]:
                raise CaseError("UNAUTHORIZED_PARTY", f"{viewer} 无权查看该案件")

        visible_items = {
            iid: item
            for iid, item in self._state["items"].items()
            if self._item_visible(item, viewer)
        }
        privileged = self._is_privileged(viewer)

        evidence_view = []
        for record in self._state["evidence"].values():
            if not self._evidence_visible(record["evidence_id"], viewer):
                continue
            entry = {
                "evidence_id": record["evidence_id"],
                "kind": record["kind"],
                "submitter": record["submitter"],
                "occurred_at": record["occurred_at"],
                "received_at": record["received_at"],
                "item_id": record["item_id"],
                "status": record["status"],
            }
            viewer_role = self._state["participants"].get(viewer)
            show_content = record["status"] != EVIDENCE_WITHDRAWN or viewer_role == ROLE_REGULATOR or viewer == record["submitter"]
            entry["content_hash"] = record["content_hash"] if show_content else None
            if record["status"] == EVIDENCE_WITHDRAWN:
                entry["withdrawn_at"] = record["withdrawn_at"]
            evidence_view.append(entry)

        commitments = [
            {
                "commitment_id": c["commitment_id"],
                "party": c["party"],
                "summary": c["summary"],
                "evidence_id": c["evidence_id"],
                "recorded_at": c["recorded_at"],
            }
            for c in self._state["commitments"].values()
            if privileged or c["party"] == viewer
        ]

        decisions_view = []
        for item in visible_items.values():
            for decision_id in item["decision_ids"]:
                d = self._state["decisions"][decision_id]
                decisions_view.append(
                    {
                        "decision_id": d["decision_id"],
                        "item_id": d["item_id"],
                        "remedy_type": d["remedy_type"],
                        "amount": str(d["amount"]),
                        "currency": d["currency"],
                        "alternative": d["alternative"],
                        "basis": list(d["basis"]),
                        "status": d["status"],
                        "proposed_by": d["proposed_by"],
                        "proposed_at": d["proposed_at"],
                        "accepted_at": d["accepted_at"],
                        "fulfilled_at": d["fulfilled_at"],
                        "superseded_at": d["superseded_at"],
                        "superseded_reason": d["superseded_reason"],
                    }
                )

        timeline = []
        for event in self._state["timeline"]:
            masked = self._mask_event(event, viewer, privileged, visible_items)
            if masked is not None:
                timeline.append(masked)

        return {
            "case": None if self._state["case"] is None else dict(self._state["case"]),
            "viewer": viewer,
            "items": [self._item_view(item) for item in visible_items.values()],
            "evidence": evidence_view,
            "commitments": commitments,
            "decisions": decisions_view,
            "timeline": timeline,
        }

    def _item_view(self, item: dict) -> dict:
        return {
            "item_id": item["item_id"],
            "title": item["title"],
            "responsible_party": item["responsible_party"],
            "action_type": item["action_type"],
            "required": item["required"],
            "due_at": item["due_at"],
            "status": item["status"],
            "claimed_by": item["claimed_by"],
            "escalation_level": len(item["escalations"]),
            "escalations": list(item["escalations"]),
            "current_decision_id": item["current_decision_id"],
            "pending_override": item["pending_override"],
            "completed_at": item["completed_at"],
        }

    def _mask_event(
        self,
        event: dict,
        viewer: str | None,
        privileged: bool,
        visible_items: dict,
    ) -> dict | None:
        etype = event["type"]
        if etype in ("CASE_OPENED", "CASE_CLOSED"):
            return dict(event)
        if etype.startswith("ITEM_") or etype in (
            "DECISION_PROPOSED",
            "DECISION_ACCEPTED",
            "REMEDY_FULFILLED",
            "ACTION_COMPLETED",
            "OVERRIDE_PROPOSED",
            "OVERRIDE_WITHDRAWN",
        ):
            item_id = event.get("item_id")
            if item_id not in visible_items:
                return None
            return dict(event)
        if etype in ("EVIDENCE_SUBMITTED", "EVIDENCE_WITHDRAWN", "REDEMPTION_RECORDED"):
            evidence_id = event.get("evidence_id") or event.get("redemption_id")
            if not self._evidence_visible(evidence_id, viewer):
                return None
            event = dict(event)
            record = self._state["evidence"][evidence_id]
            if record["status"] == EVIDENCE_WITHDRAWN and self._state["participants"].get(viewer) != ROLE_REGULATOR and viewer != record["submitter"]:
                event["content_hash"] = None
            return event
        if etype == "COMMITMENT_RECORDED":
            if not privileged and event["party"] != viewer:
                return None
            return dict(event)
        if etype in ("SETTLEMENT_BATCH_CREATED", "SETTLEMENT_BATCH_CONFIRMED"):
            if privileged:
                return dict(event)
            # 参与方只看到涉及本方的条目；消费者看到整批金额（其为收款人）。
            event = dict(event)
            if "entries" in event:
                event["entries"] = [
                    entry for entry in event["entries"] if entry["payer_party"] == viewer
                ]
                if not event["entries"]:
                    return None
            return event
        return dict(event)
