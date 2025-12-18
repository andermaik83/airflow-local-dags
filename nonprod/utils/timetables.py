from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, time
from typing import Optional

import pendulum
from croniter import croniter
from airflow.timetables.base import DataInterval, DagRunInfo, TimeRestriction, Timetable


@dataclass
class BetweenTimesCronTimetable(Timetable):
    """
    Yield cron ticks only if the local time-of-day is inside a window.
    Days-of-week and minute cadence come from the cron expression; this class
    filters by hour window (supports crossing midnight).
    """
    cron: str
    tz: str
    start_time: time       # e.g., time(4, 0)
    end_time: time         # e.g., time(2, 45)

    def _in_window(self, dt: pendulum.DateTime) -> bool:
        local = dt.in_timezone(self.tz)
        t = local.time()
        if self.start_time <= self.end_time:
            return self.start_time <= t <= self.end_time
        # Crossing midnight: allow >= start OR <= end
        return t >= self.start_time or t <= self.end_time

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        tz = pendulum.timezone(self.tz)
        # Determine starting point
        if last_automated_data_interval is None:
            base = (restriction.earliest or pendulum.now(tz)).in_timezone(tz)
        else:
            base = last_automated_data_interval.end.in_timezone(tz)

        # Iterate cron ticks until one falls inside the window (respect restriction.latest)
        latest = restriction.latest.in_timezone(tz) if restriction.latest else None
        itr = croniter(self.cron, base.naive())
        while True:
            next_dt_py: datetime = itr.get_next(datetime)
            next_dt = pendulum.instance(next_dt_py, tz=tz)
            if latest and next_dt > latest:
                return None
            if self._in_window(next_dt):
                # Use an exact interval at tick (Airflow will run the task at logical_date)
                return DagRunInfo.interval(start=next_dt, end=next_dt)

    def infer_manual_data_interval(self, *, run_after: pendulum.DateTime) -> DataInterval:
        return DataInterval.exact(run_after)