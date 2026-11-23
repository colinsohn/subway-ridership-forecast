# Frozen study periods

Declared November 21, 2026, before fitting a model or computing forecast errors. Machine-readable dates are in `study_config.json`; the development backtest now enforces these boundaries and refuses calibration/holdout input panels.

| Purpose | Local service dates |
| --- | --- |
| Development and chronological validation | Jan 1, 2025 through Aug 31, 2026 |
| Past-residual calibration for prediction ranges | Sept 1–30, 2026 |
| Final untouched holdout | Oct 1–31, 2026 |

October is the latest full calendar month before this study declaration; the partial November period is outside the study. A bounded official-source availability query found October records spanning Oct 1 00:00 through Oct 31 23:00 for the selected complexes (13,506 fare/payment rows). That query checked bounds/counts only, without examining target values or forecast errors; it does not prove complete station-hour coverage. That coverage must be audited before final evaluation. If coverage fails the declared policies, disclose the gap rather than selecting an easier replacement period.

Use the same three complexes throughout the pilot: Union Square (602), Times Square (611), and Fulton Street (628). All are Manhattan hubs, so results will not establish performance across the full NYC system. Station-hour totals include observed fare/payment partitions; missing categories can still be a source-completeness limitation. Exclude entire DST transition days and retain nulls for missing station-hours.

Validation months start March, June, September 2025 and January, April, July 2026. Each fold trains only on eligible preceding data. Forecast one service day ahead using `forecast_window.py`; reporting lag is measured in local calendar days. Compare assumed lags of 1, 3, 7, and 14 days. No feature, seasonal reference, or training label may use a service day beyond that issuance cutoff. The seasonal references planned for the first baseline are the latest available same weekday/hour and the mean of up to four previous available same weekdays/hours.

Freeze model choices using development folds. Use September residuals for range calibration without choosing models based on October. Open final holdout targets only for the December 2 evaluation milestone. Publication timestamps are not reconstructed by current revised source records, so reporting delays remain explicit simulation assumptions. Today's January and 2025 DST probes fall outside the final holdout and contain no forecast evaluation results.
