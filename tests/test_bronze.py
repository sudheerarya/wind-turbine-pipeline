import pandas as pd

def test_bronze_columns_exist():
    df = pd.read_csv('data/sample/data_group_1.csv')
    for col in ["timestamp","turbine_id","wind_speed","wind_direction","power_mw"]:
        assert col in df.columns
