from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True)
class ForecastWindow:
    """Calendar dates in the subway system's local time zone."""

    issued_on: date
    target_day: date
    observed_through: date

    def can_use_observation(self, service_day: date) -> bool:
        return service_day <= self.observed_through


def next_day_window(issued_on: date, publication_lag_days: int) -> ForecastWindow:
    """Apply an assumed delay in local calendar days to complete service days.

    The delay is a backtest parameter and does not reconstruct historical releases.
    """
    if publication_lag_days < 1:
        raise ValueError("publication_lag_days must be at least 1")

    return ForecastWindow(
        issued_on=issued_on,
        target_day=issued_on + timedelta(days=1),
        observed_through=issued_on - timedelta(days=publication_lag_days),
    )
