"""财务结算批次：跨案件按已确认结果生成，不重不漏。

"已确认结果"= 资金动作（退款/差额补发）已经 COMPLETED，即消费者侧
补救已经发生。每次结算运行：

- 扫描所有案件，把**尚未进入任何批次**的已完成资金动作各归入一个
  按案件生成的批次；
- 同一动作在案件事件流中被 ``SettlementBatchCreated`` 永久标记，
  重复运行不会再次纳入（不重）；
- 所有满足条件的动作都会被纳入，没有"被遗漏的角落"（不漏）；
- 改判后的差额补发是一个新的资金动作，完成后自然进入下一次运行。

批次本身也是案件事件，重启/崩溃后重放即可恢复，无需额外状态库。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from . import clock
from .service import CaseService
from .store import EventStore


@dataclass
class SettlementRunResult:
    run_id: str
    created_at: str
    per_case: dict[str, str] = field(default_factory=dict)   # case_id -> batch_id
    batches: list[dict[str, Any]] = field(default_factory=list)
    skipped_closed_none: list[str] = field(default_factory=list)

    @property
    def total_cents(self) -> int:
        return sum(b["total_cents"] for b in self.batches)

    @property
    def line_count(self) -> int:
        return sum(len(b["lines"]) for b in self.batches)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "created_at": self.created_at,
            "total_cents": self.total_cents,
            "line_count": self.line_count,
            "per_case": self.per_case,
            "batches": self.batches,
        }


class SettlementRunner:
    def __init__(self, store: EventStore, finance_actor: str = "finance"):
        self.store = store
        self.service = CaseService(store)
        self.finance_actor = finance_actor

    def run(self, *, run_id: str, at: datetime | None = None) -> SettlementRunResult:
        """对所有案件执行一次结算；无待结算动作的案件跳过。"""
        instant = clock.parse_ts(at) if at else clock.now_utc()
        result = SettlementRunResult(run_id=run_id, created_at=instant.isoformat())
        for case_id in self.store.list_cases():
            case = self.store.load_case(case_id)
            if not case.unsettled_fund_actions():
                continue
            batch_id = f"{run_id}-{case_id}"
            self.service.create_settlement_batch(
                case_id, actor=self.finance_actor, batch_id=batch_id, at=instant,
            )
            case = self.store.load_case(case_id)
            batch = next(b for b in case.settlement_batches if b["batch_id"] == batch_id)
            result.per_case[case_id] = batch_id
            result.batches.append({
                "case_id": case_id,
                "batch_id": batch_id,
                "currency": batch["currency"],
                "total_cents": batch["total_cents"],
                "lines": batch["lines"],
            })
        return result

    def report(self) -> dict[str, Any]:
        """全量结算报告：每个案件的动作 -> 批次覆盖情况与守恒检查。"""
        cases_report = []
        grand_settled = 0
        grand_batched = 0
        for case_id in self.store.list_cases():
            case = self.store.load_case(case_id)
            settled = case.all_settled_cents()
            batched = sum(
                line["amount_cents"]
                for b in case.settlement_batches for line in b["lines"]
            )
            covered = set(case.batched_action_ids)
            completed_fund = {
                a.action_id for a in case.actions.values()
                if a.kind in ("REFUND", "TOPUP_REFUND") and a.status == "COMPLETED"
            }
            cases_report.append({
                "case_id": case_id,
                "case_status": case.status,
                "settled_total_cents": settled,
                "batched_total_cents": batched,
                "batch_count": len(case.settlement_batches),
                "confirmed_batches": sorted(case.confirmed_batches),
                # 不重：没有动作被多个批次覆盖（事件层保证，这里复核）
                "no_duplicate": len(covered)
                                == sum(len(b["lines"]) for b in case.settlement_batches),
                # 不漏：每个已完成资金动作都已被批次覆盖
                "no_missing": completed_fund.issubset(covered),
                "unbatched_action_ids": sorted(completed_fund - covered),
            })
            grand_settled += settled
            grand_batched += batched
        return {
            "cases": cases_report,
            "settled_total_cents": grand_settled,
            "batched_total_cents": grand_batched,
            "all_balanced": grand_settled == grand_batched
                            and all(c["no_duplicate"] and c["no_missing"] for c in cases_report),
        }
