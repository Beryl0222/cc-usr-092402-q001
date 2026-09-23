"""event_consumer_guard 领域资料的基础结构。

扩展点：在原有 5 类对外交换事件之上，增加联合处置内部事件信封
（``JOINT_INTERNAL``）以及案件流转中的内部事件类型，用于把一笔投诉
关联成可追溯案件。对外合同保持向后兼容。
"""

from __future__ import annotations

#: 对外交换的事件种类（原合同，保持稳定）
EVENT_KINDS = [
    'EVENT_PUBLISHED',
    'MERCHANT_COMMITMENT',
    'BENEFIT_REDEEMED',
    'COMPLAINT_OPENED',
    'REMEDY_SETTLED',
]

#: 联合处置流转中的内部事件类型。
#:
#: CaseOpened          案件建立（关联投诉与交易凭证）
#: EvidenceSubmitted   证据到达（迟到允许，同 source+dedup_key 幂等）
#: LiabilityItemAdded  案件拆分为多个责任项
#: LiabilityClaimed    责任项被某参与方并发认领
#: DecisionRecorded    对责任项作出判罚（可改判，历史版本保留）
#: ConsumerOffered     向消费者提供部分退款/替代权益选项
#: ConsumerChose       消费者在可选项中作出选择（含拒绝）
#: ActionAssigned      为责任项分配必需补救/澄清动作，带监管时钟
#: ActionCompleted     动作完成（如已接受的退款到账）
#: ItemEscalated       某责任项单独超时升级（不影响其他责任项）
#: CaseClosed          所有必需动作完成后才能发出，案件终结
#: CaseReopened        结案后到达关键证据，案件自动重审（结案事件保留）
#: PersonalDataRedacted 消费者个人材料被请求删除/撤回，仅元数据保留
#: SettlementBatchCreated 财务按已确认结果生成批次
#: SettlementBatchConfirmed 批次复核确认（不重不漏）
JOINT_EVENT_TYPES = [
    'CaseOpened',
    'EvidenceSubmitted',
    'LiabilityItemAdded',
    'LiabilityClaimed',
    'DecisionRecorded',
    'ConsumerOffered',
    'ConsumerChose',
    'ActionAssigned',
    'ActionCompleted',
    'ItemEscalated',
    'CaseClosed',
    'CaseReopened',
    'PersonalDataRedacted',
    'SettlementBatchCreated',
    'SettlementBatchConfirmed',
]

#: 内部事件信封的必填字段
JOINT_ENVELOPE_REQUIRED = ("case_id", "seq", "event_type", "occurred_at", "actor", "data")

#: 对外事件信封的必填字段（原合同）
REQUIRED_FIELDS = ("event_id", "kind", "occurred_at", "subject_id", "payload")


def validate_event(record: dict) -> list[str]:
    """检查样例事件是否具备可交换的最小字段。"""
    problems = [name for name in REQUIRED_FIELDS if name not in record]
    if record.get("kind") not in EVENT_KINDS:
        problems.append("kind")
    return problems


def validate_joint_envelope(record: dict) -> list[str]:
    """检查联合处置内部事件信封的最小字段。

    与 ``validate_event`` 一样返回问题列表，空列表表示合格。
    同时校验时间戳可解析、序号为非负整数、事件类型已知。
    """
    problems = [name for name in JOINT_ENVELOPE_REQUIRED if name not in record]
    if record.get("event_type") not in JOINT_EVENT_TYPES:
        problems.append("event_type")
    if not isinstance(record.get("seq"), int) or isinstance(record.get("seq"), bool) or record.get("seq") < 0:
        problems.append("seq")
    from datetime import datetime

    ts = record.get("occurred_at")
    aware = isinstance(ts, datetime)
    if not aware:
        parsed = _parse_iso(ts) if isinstance(ts, str) else None
        if parsed is None:
            problems.append("occurred_at")
        elif parsed.tzinfo is None:
            # 跨时区协作里朴素时间有歧义，必须带时区
            problems.append("occurred_at")
    elif ts.tzinfo is None:
        problems.append("occurred_at")
    return problems


def _parse_iso(value: str):
    from datetime import datetime

    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
