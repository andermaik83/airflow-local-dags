from __future__ import annotations
from dataclasses import dataclass
from datetime import time, datetime
from typing import Optional

import pendulum
from airflow.timetables.base import DataInterval, DagRunInfo, TimeRestriction, Timetable
from airflow.timetables.simple import CronDataIntervalTimetable


@dataclass
class BetweenTimesCronTimetable(Timetable):
    """
    A timetable that emits runs following a cron expression, but only when the
    run's start time falls inside a daily time window (supports crossing midnight).

    Examples:
        - cron: "*/5 * * * 1-6" (Mon–Sat every 5 minutes)
        - start_time: time(4, 0)
        - end_time:   time(2, 45)
        - tz: "Europe/Brussels" (use this for Belgium; "Europe/Antwerp" is not an IANA zone)
    """

    cron: str
    tz: str
    start_time: time
    end_time: time

    def __post_init__(self) -> None:
        # Prepare timezone and underlying cron timetable
        self._tz = pendulum.timezone(self.tz)
        self._base = CronDataIntervalTimetable(self.cron, timezone=self.tz)

    def _in_window(self, dt: pendulum.DateTime) -> bool:
        local = dt.in_timezone(self._tz)
        t = local.time()
        if self.start_time <= self.end_time:
            # Non-crossing window
            return self.start_time <= t <= self.end_time
        # Crossing midnight: [start, 24h) U [00:00, end]
        return t >= self.start_time or t <= self.end_time

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        # Use Airflow's cron timetable to get candidate runs, and filter by the window
        current = last_automated_data_interval
        while True:
            info = self._base.next_dagrun_info(
                last_automated_data_interval=current, restriction=restriction
            )
            if info is None:
                return None
            # Filter by the logical_date (the tick instant), not the data interval
            if self._in_window(info.logical_date):
                return info
            current = info.data_interval

    def infer_manual_data_interval(self, *, run_after: pendulum.DateTime) -> DataInterval:
        # Delegate to underlying cron timetable for manual runs
        return self._base.infer_manual_data_interval(run_after=run_after)
