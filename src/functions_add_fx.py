import pandas as pd
import os
import re

def get_opening_fx(currency, year, df_fx):
    date=str(int(year)-1)+"-12-01"
    
    try:
        value=df_fx[
        (df_fx["D_CU"] == currency) &
        (df_fx["D_PE"] == date)
        ].TC.values[0]
    
    except:
        return None
    
    return value