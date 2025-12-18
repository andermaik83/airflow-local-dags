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
	A timetable that emits cron ticks only when the local time-of-day is inside
	a daily window (supports crossing midnight). Days-of-week and cadence come
	from the cron expression; this class filters by the time window only.
	"""

	cron: str
	tz: str
	start_time: time
	end_time: time

	def _in_window(self, dt: pendulum.DateTime) -> bool:
		local = dt.in_timezone(self.tz)
		t = local.time()
		if self.start_time <= self.end_time:
			return self.start_time <= t <= self.end_time
		# Cross-midnight window: allow >= start OR <= end
		return t >= self.start_time or t <= self.end_time

	def next_dagrun_info(
		self,
		*,
		last_automated_data_interval: Optional[DataInterval],
		restriction: TimeRestriction,
	) -> Optional[DagRunInfo]:
		tz = pendulum.timezone(self.tz)
		if last_automated_data_interval is None:
			base = (restriction.earliest or pendulum.now(tz)).in_timezone(tz)
		else:
			base = last_automated_data_interval.end.in_timezone(tz)

		latest = restriction.latest.in_timezone(tz) if restriction.latest else None
		itr = croniter(self.cron, base.naive())
		for _ in range(1000):  # safety cap
			next_py: datetime = itr.get_next(datetime)
			next_dt = pendulum.instance(next_py, tz=tz)
			if latest and next_dt > latest:
				return None
			if self._in_window(next_dt):
				return DagRunInfo.logical_date(next_dt)
		return None

	def infer_manual_data_interval(self, *, run_after: pendulum.DateTime) -> DataInterval:
		# Use instantaneous interval at run_after
		return DataInterval(start=run_after, end=run_after)