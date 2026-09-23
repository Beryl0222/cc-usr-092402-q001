"""跨时区处理时钟。

所有截止时间在内部都以**带时区的绝对时间点**（UTC 可比）保存；
参与方所在时区只影响展示与本地墙钟录入，不影响超时判定本身。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

try:  # 平台带 tzdata 时优先使用 IANA 时区
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - 标准环境均可用
    ZoneInfo = None  # type: ignore[assignment]


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_ts(value: datetime | str) -> datetime:
    """把 ISO8601 字符串解析为带时区时间点。

    朴素时间（没有时区信息）一律拒绝——跨时区协作里它有歧义；
    调用方必须明确时区后再录入。
    """
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"无法解析时间戳: {value!r}") from exc
    if dt.tzinfo is None:
        raise ValueError(f"时间戳缺少时区信息: {value!r}")
    return dt


def instant_from_local(wall_time: datetime | str, tz_name: str) -> datetime:
    """把某时区下的本地墙钟时间转换为绝对时间点。"""
    if ZoneInfo is None:  # pragma: no cover
        raise RuntimeError("当前环境不支持 IANA 时区")
    tz = ZoneInfo(tz_name)
    if isinstance(wall_time, str):
        wall_time = datetime.fromisoformat(wall_time)
    if wall_time.tzinfo is not None:
        # 已带时区：以其绝对时刻为准，忽略 tz_name，避免歧义调用。
        return wall_time.astimezone(timezone.utc)
    return wall_time.replace(tzinfo=tz).astimezone(timezone.utc)


def to_local(instant: datetime, tz_name: str) -> datetime:
    """把绝对时间点换算到目标时区的墙钟时间（仍带 tzinfo）。"""
    if ZoneInfo is None:  # pragma: no cover
        raise RuntimeError("当前环境不支持 IANA 时区")
    return parse_ts(instant).astimezone(ZoneInfo(tz_name))


def deadline_after(start: datetime, duration: timedelta) -> datetime:
    """从起始绝对时间点经过固定时长得到截止时间点。

    时长按物理时间计算，不随任何参与方的夏令时/UTC 偏移跳变。
    """
    return parse_ts(start) + duration


def is_overdue(deadline: datetime, at: datetime) -> bool:
    return parse_ts(at) > parse_ts(deadline)


def format_in_zone(instant: datetime, tz_name: str) -> str:
    return to_local(instant, tz_name).isoformat()
