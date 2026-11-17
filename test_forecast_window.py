import unittest
from datetime import date

from forecast_window import next_day_window


class ForecastWindowTests(unittest.TestCase):
    def test_delay_applies_to_inputs_not_target(self):
        window = next_day_window(date(2025, 1, 1), publication_lag_days=10)

        self.assertEqual(window.target_day, date(2025, 1, 2))
        self.assertEqual(window.observed_through, date(2024, 12, 22))
        self.assertTrue(window.can_use_observation(date(2024, 12, 22)))
        self.assertFalse(window.can_use_observation(date(2024, 12, 23)))
        self.assertFalse(window.can_use_observation(window.target_day))

    def test_same_day_data_is_excluded(self):
        window = next_day_window(date(2025, 3, 8), publication_lag_days=1)

        self.assertEqual(window.target_day, date(2025, 3, 9))
        self.assertTrue(window.can_use_observation(date(2025, 3, 7)))
        self.assertFalse(window.can_use_observation(date(2025, 3, 8)))

    def test_invalid_delay_is_rejected(self):
        for lag in (-1, 0):
            with self.subTest(lag=lag), self.assertRaises(ValueError):
                next_day_window(date(2025, 1, 1), publication_lag_days=lag)
