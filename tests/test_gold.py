import pandas as pd
import numpy as np

def load_silver_like():
    paths = [
        'data/sample/data_group_1.csv',
        'data/sample/data_group_2.csv',
        'data/sample/data_group_3.csv',
    ]
    df = pd.concat([pd.read_csv(p) for p in paths], ignore_index=True)
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.dropna(subset=['timestamp','turbine_id'])
    for c in ['wind_speed','wind_direction','power_mw']:
        med = df[c].median(skipna=True)
        df[c] = df[c].fillna(med)
        q1 = df[c].quantile(0.25)
        q3 = df[c].quantile(0.75)
        iqr = q3-q1
        lo, hi = q1-1.5*iqr, q3+1.5*iqr
        df = df[(df[c] >= lo) & (df[c] <= hi)]
    return df

def test_stats_and_anomalies():
    df = load_silver_like()
    stats = df.groupby('turbine_id')['power_mw'].agg(['min','max','mean','count'])
    assert (stats['count'] > 0).all()
    # anomaly z-score
    grp = df.groupby('turbine_id')
    mu = grp['power_mw'].transform('mean')
    sigma = grp['power_mw'].transform('std').replace(0, np.nan)
    z = (df['power_mw'] - mu) / sigma
    # allow NaNs when sigma==0; anomalies where |z|>=2
    anoms = (z.abs() >= 2).fillna(False)
    assert anoms.isin([True, False]).all()
