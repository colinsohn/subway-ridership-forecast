import copy
from datetime import date, datetime, timedelta
import json
from pathlib import Path
import tempfile
import unittest

import duckdb

from aggregate import aggregate, calendar_hours
from backtest import (Observation, SeasonalHistory, chronological_folds, forecast,
                      load_panel, prediction_rows, run, validate_config)
from forecast_window import next_day_window
from ingest import Selection, ValidationError, download
from test_ingest import FakeSource, row


def config():
    return {"format_version":1,"timezone":"America/New_York","dst_policy":"exclude_entire_transition_day",
        "missing_policy":"no_zero_imputation","station_complex_ids":["611"],"publication_lag_days":[1,7,14],
        "development_start":"2025-01-01","development_end_exclusive":"2025-03-01",
        "calibration_start":"2025-03-01","calibration_end_exclusive":"2025-04-01",
        "holdout_start":"2025-04-01","holdout_end_exclusive":"2025-05-01",
        "validation_month_starts":["2025-02-01"]}


def observation(day,value,station="611",hour=8,status="observed"):
    return Observation(station,datetime.combine(date.fromisoformat(day),datetime.min.time()).replace(hour=hour),value,status)


class BacktestTests(unittest.TestCase):
    def test_folds_and_rejected_periods(self):
        c=config();fold=chronological_folds(c)[0]
        self.assertEqual(fold['validation_end_exclusive'],'2025-03-01')
        self.assertEqual(fold['first_observed_through']['1'],'2025-01-30')
        self.assertEqual(fold['first_observed_through']['14'],'2025-01-17')
        for update in [dict(validation_month_starts=['2025-03-01']),dict(validation_month_starts=['2025-02-02']),
            dict(validation_month_starts=['2025-02-01','2025-02-01']),dict(calibration_start='2025-03-02'),
            dict(publication_lag_days=[0]),dict(publication_lag_days=[True])]:
            bad=copy.deepcopy(c);bad.update(update)
            with self.assertRaises(ValidationError):validate_config(bad)

    def test_exact_cutoff_weekday_and_mean(self):
        observations=[observation('2025-03-10',10),observation('2025-03-03',20),
            observation('2025-02-24',30),observation('2025-02-17',40),
            observation('2025-03-16',999),observation('2025-03-17',9999)]
        h=SeasonalHistory(observations)
        result=forecast(h,'611',8,date(2025,3,16),1)
        self.assertEqual(result['seasonal_latest'][0],10)
        self.assertEqual(result['seasonal_mean4'][0],25)
        # Seven-day reporting lag means the immediately preceding Monday is
        # unavailable at Sunday's issuance, so use March 3, not March 10.
        result=forecast(h,'611',8,date(2025,3,16),7)
        self.assertEqual(result['seasonal_latest'][0],20)
        self.assertEqual(result['seasonal_latest'][1][0][0],date(2025,3,3))
        self.assertEqual(forecast(h,'611',8,date(2025,3,16),14)['seasonal_latest'][0],30)
        window=next_day_window(date(2025,3,16),6)  # cutoff exactly March 10
        self.assertEqual(h.references('611',8,window)[0][0],window.observed_through)

    def test_future_perturbation_missing_zero_and_station_isolation(self):
        old=[observation('2025-03-03',0),observation('2025-03-10',None,status='missing'),
             observation('2025-03-09',None,status='excluded_dst_day'),observation('2025-03-10',500,'602')]
        expected=forecast(SeasonalHistory(old),'611',8,date(2025,3,16),7)
        changed=old+[observation('2025-03-17',1e10),observation('2025-03-24',1e11)]
        self.assertEqual(forecast(SeasonalHistory(changed),'611',8,date(2025,3,16),7),expected)
        self.assertEqual(expected['seasonal_latest'][0],0)
        self.assertIsNone(forecast(SeasonalHistory(old),'999',8,date(2025,3,16),1)['seasonal_latest'][0])
        with self.assertRaises(ValidationError):SeasonalHistory([old[0],old[0]])
        with self.assertRaises(ValidationError):SeasonalHistory([observation('2025-03-03',1,status='excluded_dst_day')])

    def test_walk_forward_references_and_same_scoring_cases(self):
        c=config();obs=[Observation('611',stamp,0 if stamp.day==1 else float(stamp.day),'observed')
                        for stamp,_,_,_,_ in calendar_hours(date(2025,1,1),date(2025,3,1))]
        rows=list(prediction_rows(obs,c))
        self.assertEqual(len(rows),28*24*3*2)
        for record in rows:
            self.assertLess(record['reference_newest'],record['target_day'])
            self.assertLessEqual(record['reference_newest'],record['observed_through'])
            self.assertLess(record['observed_through'],record['issued_on'])
        # A later validation issuance can use earlier validation observations
        # once their reporting delay has elapsed (daily walk-forward).
        one=next(r for r in rows if r['target_day']=='2025-02-22' and r['lag_days']==1 and r['method']=='seasonal_latest')
        self.assertEqual(one['reference_newest'],'2025-02-15')
        obs.append(observation('2025-04-01',1))
        with self.assertRaises(ValidationError):list(prediction_rows(obs,c))
        incomplete=obs[:-2]
        with self.assertRaises(ValidationError):list(prediction_rows(incomplete,c))

    def test_transition_day_forecasts_are_excluded(self):
        c=config()
        c.update(development_end_exclusive='2025-04-01',calibration_start='2025-04-01',
                 calibration_end_exclusive='2025-05-01',holdout_start='2025-05-01',
                 holdout_end_exclusive='2025-06-01',validation_month_starts=['2025-03-01'])
        obs=[Observation('611',stamp,None if excluded else 5.0,'excluded_dst_day' if excluded else 'observed')
             for stamp,_,_,_,excluded in calendar_hours(date(2025,1,1),date(2025,4,1))]
        rows=list(prediction_rows(obs,c))
        transition=[r for r in rows if r['target_day']=='2025-03-09']
        self.assertEqual(len(transition),24*3*2)
        self.assertTrue(all(r['prediction'] is None and not r['scoreable'] for r in transition))
        self.assertTrue(all(r['reference_newest']!='2025-03-09' for r in rows))

    def test_real_pipeline_metrics_checksums_and_holdout_guard(self):
        c=config()
        # One observed zero per day; all remaining hours stay null. This checks
        # metric denominators, no imputation, and undefined zero-denominator WAPE.
        source=[row(f'row-{i}',f'{day}T08:00:00.000',ridership='0',transfers='0')
                for i,day in enumerate(date(2025,1,1)+timedelta(days=n) for n in range(59))]
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);snapshot=root/'snapshot';panel=root/'panel';output=root/'evaluation'
            download(Selection(date(2025,1,1),date(2025,3,1),('611',)),snapshot,fetch=FakeSource(source))
            aggregate(snapshot,panel)
            report=run(panel,output,c)
            self.assertEqual(report['chronology_violations'],0)
            for metric in report['overall']:
                self.assertEqual(metric['scheduled'],28*24)
                self.assertEqual(metric['scored'],28)
                self.assertEqual(metric['missing_targets'],28*23)
                self.assertEqual(metric['mae'],0)
                self.assertEqual(metric['rmse'],0)
                self.assertIsNone(metric['wape'])
            with duckdb.connect() as db:
                self.assertEqual(db.execute('SELECT count(*) FROM read_parquet(?)',[str(output/'predictions.parquet')]).fetchone()[0],28*24*3*2)
            self.assertEqual(json.loads((output/'metrics.json').read_text()),report)
            with self.assertRaises(FileExistsError):run(panel,output,c)
            # A selection extending into reserved periods is rejected even
            # before row loading. The artifacts themselves remain unmodified.
            audit_path=panel/'audit.json';audit=json.loads(audit_path.read_text())
            audit['selection']['end_exclusive']='2025-04-02';audit_path.write_text(json.dumps(audit))
            with self.assertRaisesRegex(ValidationError,'calibration/holdout'):load_panel(panel,c)
            audit['selection']['end_exclusive']='2025-03-01';audit_path.write_text(json.dumps(audit))
            with (panel/'station_hours.parquet').open('ab') as stream:stream.write(b'tampered')
            with self.assertRaisesRegex(ValidationError,'checksum'):load_panel(panel,c)


if __name__=='__main__':unittest.main()
