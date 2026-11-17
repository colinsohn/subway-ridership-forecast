# Subway Ridership Forecast

A project to forecast next-day hourly subway entries at selected New York City station complexes. The evaluation will compare simple weekly patterns with a machine-learning model while simulating the delay between when trips occur and when their counts become available.

## Current status

The current code defines when an observation is eligible as a model input. It uses a configurable delay in local calendar days. Data ingestion, the models, and the backtest are under development.

Run the current tests with Python 3.9 or later.

```sh
python -m unittest -v
```

Data source: [MTA Subway Hourly Ridership](https://catalog.data.gov/dataset/mta-subway-hourly-ridership-beginning-2025).
