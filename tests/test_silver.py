import pandas as pd
import numpy as np

def load_all():
    paths = [
        'data/sample/data_group_1.csv',
        'data/sample/data_group_2.csv',
        'data/sample/data_group_3.csv',
    ]
    parts = [pd.read_csv(p) for p in paths]
    df = pd.concat(parts, ignore_index=True)
    # basic cleaning mirroring Spark stage
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

def test_no_nulls_in_numeric_after_impute():
    df = load_all()
    assert df[['wind_speed','wind_direction','power_mw']].isna().sum().sum() == 0
