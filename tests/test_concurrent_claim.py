"""场景一：并发认领。

多个参与方（或同一参与方的多个请求）同时认领同一责任项时，
必须恰好有一个成功，其余得到明确的 AlreadyClaimed；
同一案件上的其他并发写入（不同责任项）不能被丢失或阻塞。
"""

from __future__ import annotations

import threading
import unittest

from src.jointcase.domain import AlreadyClaimed, Conflict
from src.jointcase.store import EventStore
from src.jointcase.service import CaseService

from tests._helpers import build_two_item_case


class ConcurrentClaimTest(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.tmp = tempfile.TemporaryDirectory()
        self.ctx = build_two_item_case(self.tmp.name, case_id="C-CLAIM")
        # 再加一个未被认领的责任项 I3 供并发竞争
        self.svc = self.ctx["svc"]
        self.svc.submit_evidence("C-CLAIM", actor="agent", evidence_id="E3",
                                 source_party="organizer", kind="EVENT_PUBLISHED",
                                 content={})
        self.svc.add_item("C-CLAIM", actor="agent", item_id="I3",
                          title="安保疏导责任", linked_evidence=["E3"])

    def tearDown(self):
        self.tmp.cleanup()

    def test_exactly_one_winner_under_concurrency(self):
        parties = ["merchant", "platform", "organizer", "merchant", "platform"]
        results: dict[int, object] = {}
        errors: dict[int, BaseException] = {}
        barrier = threading.Barrier(len(parties))

        def worker(idx: int, party: str):
            # 每个线程使用独立的 service/store 实例，模拟不同进程节点
            local_svc = CaseService(EventStore(self.tmp.name))
            barrier.wait()
            try:
                local_svc.claim_item("C-CLAIM", party_id=party, item_id="I3")
                results[idx] = "won"
            except AlreadyClaimed as exc:
                errors[idx] = exc

        threads = [threading.Thread(target=worker, args=(i, p))
                   for i, p in enumerate(parties)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        winners = [idx for idx, outcome in results.items() if outcome == "won"]
        losers = list(errors)
        self.assertEqual(len(winners), 1, f"应有且仅有一个认领成功方，实际: {winners=}")
        self.assertEqual(len(losers), len(parties) - 1)

        case = EventStore(self.tmp.name).load_case("C-CLAIM")
        winner_party = case.items["I3"].claimed_by
        self.assertIn(winner_party, parties)
        # 失败方重放后看到的归属与获胜方一致
        winning_idx = winners[0]
        self.assertEqual(parties[winning_idx], winner_party)

    def test_concurrent_writes_on_different_items_are_not_lost(self):
        """两个节点并发在不同责任项上完成动作，两个更新都必须落盘。"""
        svc_a = CaseService(EventStore(self.tmp.name))
        svc_b = CaseService(EventStore(self.tmp.name))
        done = []

        def complete_a():
            svc_a.complete_action("C-CLAIM", actor="merchant", action_id="A1")
            done.append("A1")

        def complete_b():
            svc_b.complete_action("C-CLAIM", actor="organizer", action_id="A2")
            done.append("A2")

        t1 = threading.Thread(target=complete_a)
        t2 = threading.Thread(target=complete_b)
        t1.start(); t2.start()
        t1.join(timeout=10); t2.join(timeout=10)

        case = EventStore(self.tmp.name).load_case("C-CLAIM")
        self.assertEqual(case.actions["A1"].status, "COMPLETED")
        self.assertEqual(case.actions["A2"].status, "COMPLETED")
        self.assertEqual(sorted(done), ["A1", "A2"])
        # 事件序号连续无空洞
        seqs = [e["seq"] for e in case.events]
        self.assertEqual(seqs, list(range(len(seqs))))

    def test_stale_expected_seq_raises_conflict(self):
        """直接用过期序号追加必须被 CAS 拒绝（不覆盖他人事件）。"""
        store = EventStore(self.tmp.name)
        case = store.load_case("C-CLAIM")
        stale_seq = case.seq
        # 别的参与方先追加一条
        CaseService(EventStore(self.tmp.name)).submit_evidence(
            "C-CLAIM", actor="agent", evidence_id="E-LATE",
            source_party="platform", kind="BENEFIT_REDEEMED", content={})
        with self.assertRaises(Conflict):
            store.append(
                "C-CLAIM",
                [("EvidenceSubmitted", {"x": 1})],
                actor="agent", expected_seq=stale_seq,
            )


if __name__ == "__main__":
    unittest.main()
