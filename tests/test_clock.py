"""跨时区处理时钟测试。

监管时限以带时区的绝对时间点比较；参与方在不同城市录入的本地截止时间
换算结果一致；朴素时间戳一律被拒绝以消除歧义。
"""

from __future__ import annotations

import unittest
from datetime import timedelta

from src.jointcase import clock


class ClockTest(unittest.TestCase):
    def test_naive_timestamp_rejected(self):
        with self.assertRaises(ValueError):
            clock.parse_ts("2026-09-23T09:00:00")

    def test_aware_timestamp_roundtrips_utc(self):
        dt = clock.parse_ts("2026-09-23T09:00:00+08:00")
        self.assertEqual(dt.utcoffset(), timedelta(hours=8))
        self.assertEqual(dt.isoformat(), "2026-09-23T09:00:00+08:00")

    def test_same_instant_in_three_zones(self):
        # 监管要求"赛事当地时间 9月23日 18:00 前"给出澄清
        beijing = clock.instant_from_local("2026-09-23T18:00:00", "Asia/Shanghai")
        # 伦敦的值班同事看到同一截止点是 11:00（9月英国夏令时 UTC+1）
        london_wall = clock.to_local(beijing, "Europe/London")
        self.assertEqual(london_wall.strftime("%Y-%m-%d %H:%M"), "2026-09-23 11:00")
        # 洛杉矶的平台运营看到的是 03:00（PDT, UTC-7）
        la_wall = clock.to_local(beijing, "America/Los_Angeles")
        self.assertEqual(la_wall.strftime("%Y-%m-%d %H:%M"), "2026-09-23 03:00")
        # 绝对时刻相同
        self.assertEqual(
            clock.to_local(beijing, "UTC"),
            clock.to_local(clock.parse_ts(london_wall.isoformat()), "UTC"),
        )

    def test_deadline_is_physical_duration(self):
        start = clock.instant_from_local("2026-11-01T00:00:00", "America/New_York")
        # 跨美国夏令时结束（11月1日 2:00 拨回一小时），48 小时仍是 48 物理小时
        deadline = clock.deadline_after(start, timedelta(hours=48))
        self.assertEqual((deadline - start), timedelta(hours=48))

    def test_overdue_comparison_uses_instant_not_wallclock(self):
        deadline = clock.instant_from_local("2026-09-23T18:00:00", "Asia/Shanghai")
        check_from_london = clock.instant_from_local("2026-09-23T11:30:00", "Europe/London")
        self.assertTrue(clock.is_overdue(deadline, check_from_london))
        check_early = clock.instant_from_local("2026-09-23T10:00:00", "Europe/London")
        self.assertFalse(clock.is_overdue(deadline, check_early))


if __name__ == "__main__":
    unittest.main()
