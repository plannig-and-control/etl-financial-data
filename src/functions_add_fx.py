import pandas as pd
import os
import re

def get_opening_fx(currency, year, df_fx):
    date=str(int(year)-1)+"-12-01"
    
    try:
        value= df_fx[
            (df_fx["D_CU"] == currency) &
            (df_fx["D_PE"] == date)
            ].TC.values[0]
    
    except:
        return None
    
    return value

def process_fx(path_fx):
    #read and process FX files
    #eliminate budget rows
    df_fx=pd.read_excel(path_fx, sheet_name="FX", parse_dates=["PERIOD"])
    index_drop = df_fx[df_fx.Scenario == "Budget"].index
    df_fx.drop(index_drop, inplace=True)
    df_fx.reset_index(inplace=True, drop=True)

    #rename columns
    df_fx = df_fx.rename(columns={"FX_RATE_FINAL": "TC", "FX_RATE_AVG": "TMN", "PERIOD": "D_PE", "CURRENCY": "D_CU"})

    #eliminate rows with 2019 values
    index_drop = df_fx[df_fx.D_PE < "2020-01-01"].index
    df_fx.drop(index_drop, inplace=True)
    df_fx.reset_index(inplace=True, drop=True)

    #get opening FX column
    df_fx["TO"] = df_fx["D_CU"].apply(lambda x: get_opening_fx(x,"2020",df_fx))
    df_fx.loc[df_fx['D_PE'] < '2020-01-01', 'TO'] = None

    return df_fx