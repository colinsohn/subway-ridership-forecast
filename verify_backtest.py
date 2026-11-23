"""Independently check saved seasonal forecasts by stepping backward in weeks."""
import argparse
from datetime import date, timedelta
import json
import math
from pathlib import Path

import duckdb

from ingest import sha256

if not __debug__:
    raise RuntimeError("run verification without Python -O; assertions must be enabled")


def verify(panel, evaluation):
    panel, evaluation = Path(panel), Path(evaluation)
    report = json.loads((evaluation/'metrics.json').read_text())
    if sha256(evaluation/'predictions.parquet') != report['predictions_parquet_sha256']:
        raise ValueError('prediction checksum mismatch')
    if sha256(panel/'station_hours.parquet') != report['panel_parquet_sha256']:
        raise ValueError('panel checksum mismatch')
    with duckdb.connect() as db:
        history = {(station,stamp.date(),stamp.hour):(value,status) for station,stamp,value,status in db.execute(
            'SELECT station_complex_id,local_timestamp,target,status FROM read_parquet(?)',
            [str(panel/'station_hours.parquet')]).fetchall()}
        records = db.execute('SELECT station_complex_id,target_day,hour,issued_on,observed_through,lag_days,method,actual,prediction,reference_count,reference_oldest,reference_newest,scoreable FROM read_parquet(?)',
            [str(evaluation/'predictions.parquet')]).fetchall()
    floor = date.fromisoformat(report['study_config']['development_start'])
    sums, checked = {}, 0
    for station,target,hour,issued,cutoff,lag,method,actual,prediction,nrefs,oldest,newest,scoreable in records:
        assert issued == target-timedelta(days=1)
        assert cutoff == issued-timedelta(days=lag)
        value,status = history[(station,target,hour)]
        assert actual == value
        refs, candidate = [], target-timedelta(days=7)
        if status != 'excluded_dst_day':
            while candidate >= floor and len(refs)<4:
                observation = history.get((station,candidate,hour),(None,'missing'))[0]
                if candidate <= cutoff and observation is not None:
                    refs.append((candidate,observation))
                candidate -= timedelta(days=7)
        if method == 'seasonal_latest':refs=refs[:1]
        elif method != 'seasonal_mean4':raise ValueError('unexpected baseline method')
        expected = sum(v for _,v in refs)/len(refs) if refs else None
        assert nrefs == len(refs)
        assert oldest == (refs[-1][0] if refs else None)
        assert newest == (refs[0][0] if refs else None)
        if expected is None:assert prediction is None
        else:assert math.isclose(prediction,expected,rel_tol=1e-12,abs_tol=1e-9)
        assert scoreable == (actual is not None and prediction is not None)
        if scoreable:
            total=sums.setdefault((lag,method),[0,0.0,0.0,0.0])
            total[0]+=1;total[1]+=abs(prediction-actual);total[2]+=(prediction-actual)**2;total[3]+=actual
        checked+=1
    for metric in report['overall']:
        count,absolute,squared,actual_sum = sums[(metric['lag_days'],metric['method'])]
        assert metric['scored']==count
        for key,expected in [('mae',absolute/count),('rmse',math.sqrt(squared/count)),('wape',absolute/actual_sum if actual_sum else None)]:
            if expected is None:assert metric[key] is None
            else:assert math.isclose(metric[key],expected,rel_tol=1e-10,abs_tol=1e-9)
    return {'prediction_rows_checked':checked,'reference_or_metric_discrepancies':0,
            'method':'independent seven-day backward search plus Python metric recalculation',
            'source_rows_sha256':report['input_source_rows_sha256'],
            'predictions_parquet_sha256':report['predictions_parquet_sha256']}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('panel',type=Path);parser.add_argument('evaluation',type=Path)
    args=parser.parse_args()
    print(json.dumps(verify(args.panel,args.evaluation),indent=2))
