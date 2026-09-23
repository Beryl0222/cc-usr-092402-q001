"""追加式事件存储。

每个案件一个 JSONL 追加日志（``<store_dir>/<case_id>.events.jsonl``），
事件按全局 ``seq`` 顺序排列。并发安全由两层保证：

1. **文件锁**（``fcntl.flock``）：同一时刻只有一个进程/线程能在案件日志上
   追加，覆盖并发认领、并发判罚等竞争；
2. **期望序号 CAS**：调用方基于重放得到的 ``case.seq`` 提交，落盘前再次
   核对，序号已被抢先则抛 :class:`Conflict`，由服务层重放重试。

只追加、不修改：改判、撤回都以新事件表达，因此进程崩溃/服务重启后
重放日志即可完整恢复，日志本身也是案件审计轨迹。
"""

from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
from typing import Any, Iterable

from .domain import Case, Conflict, JointCaseError


class EventStore:
    """文件系统上的每案件一日志事件存储。"""

    def __init__(self, store_dir: str | os.PathLike):
        self.dir = Path(store_dir)
        self.dir.mkdir(parents=True, exist_ok=True)

    # -- 路径 --------------------------------------------------------------

    def _path(self, case_id: str) -> Path:
        if not case_id or "/" in case_id or os.sep in case_id or ".." in case_id:
            raise JointCaseError(f"非法案件标识: {case_id!r}")
        return self.dir / f"{case_id}.events.jsonl"

    def _lock_path(self, case_id: str) -> Path:
        return self.dir / f".{case_id}.lock"

    # -- 读取 --------------------------------------------------------------

    def exists(self, case_id: str) -> bool:
        return self._path(case_id).exists()

    def load_events(self, case_id: str) -> list[dict[str, Any]]:
        path = self._path(case_id)
        if not path.exists():
            from .domain import NotFound
            raise NotFound(f"案件日志不存在: {case_id}")
        events: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    raise JointCaseError(
                        f"案件 {case_id} 日志第 {lineno} 行损坏"
                    ) from exc
        return events

    def load_case(self, case_id: str) -> Case:
        return Case.replay(self.load_events(case_id))

    def list_cases(self) -> list[str]:
        return sorted(
            p.name[: -len(".events.jsonl")]
            for p in self.dir.glob("*.events.jsonl")
        )

    # -- 追加 --------------------------------------------------------------

    def append(self, case_id: str, pending: Iterable[tuple[str, dict]], *,
               actor: str, expected_seq: int,
               occurred_at: str | None = None) -> list[dict[str, Any]]:
        """在持锁状态下核对序号并连续追加一批事件。

        ``pending`` 是同一次命令产生的一或多个 ``(event_type, data)``；
        返回落盘的完整事件信封。序号不匹配时抛 :class:`Conflict`，
        不会写入任何事件。
        """
        from .clock import now_utc

        path = self._path(case_id)
        lock_path = self._lock_path(case_id)
        pending = list(pending)
        if not pending:
            return []
        ts = occurred_at or now_utc().isoformat()
        stored: list[dict[str, Any]] = []
        with open(lock_path, "w", encoding="utf-8") as lock_fh:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
            try:
                current_seq = self._last_seq(path)
                if current_seq != expected_seq:
                    raise Conflict(
                        f"案件 {case_id} 序号冲突：期望 {expected_seq}，"
                        f"日志当前 {current_seq}（已被其他参与方抢先追加）"
                    )
                seq = current_seq
                with path.open("a", encoding="utf-8") as fh:
                    for event_type, data in pending:
                        envelope = {
                            "case_id": case_id,
                            "seq": seq,
                            "event_type": event_type,
                            "occurred_at": ts,
                            "actor": actor,
                            "data": data,
                        }
                        fh.write(json.dumps(envelope, ensure_ascii=False, sort_keys=True) + "\n")
                        stored.append(envelope)
                        seq += 1
                    fh.flush()
                    os.fsync(fh.fileno())
            finally:
                fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
        return stored

    def init_case(self, case_id: str, opened: tuple[str, dict], *, actor: str,
                  occurred_at: str | None = None) -> Case:
        """建案：要求日志尚不存在，原子写入 CaseOpened。"""
        path = self._path(case_id)
        lock_path = self._lock_path(case_id)
        with open(lock_path, "w", encoding="utf-8") as lock_fh:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
            try:
                if path.exists():
                    raise Conflict(f"案件已存在: {case_id}")
                from .clock import now_utc
                envelope = {
                    "case_id": case_id,
                    "seq": 0,
                    "event_type": opened[0],
                    "occurred_at": occurred_at or now_utc().isoformat(),
                    "actor": actor,
                    "data": opened[1],
                }
                with path.open("w", encoding="utf-8") as fh:
                    fh.write(json.dumps(envelope, ensure_ascii=False, sort_keys=True) + "\n")
                    fh.flush()
                    os.fsync(fh.fileno())
            finally:
                fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
        return self.load_case(case_id)

    # -- 内部 --------------------------------------------------------------

    @staticmethod
    def _last_seq(path: Path) -> int:
        """返回日志中下一事件应使用的序号。

        按文本逐行扫描取最后一条完整 JSON；不能按固定字节块从尾部切，
        因为事件正文含多字节 UTF-8 字符（中文），块首可能落在字符中间。
        """
        if not path.exists() or path.stat().st_size == 0:
            return 0
        last_line = ""
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    last_line = line
        if not last_line:
            return 0
        return json.loads(last_line)["seq"] + 1
