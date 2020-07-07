import pandas as pd
from functions_general import *
from functions_add_fx import *
from variables import *
from datetime import datetime
from dateutil.relativedelta import relativedelta
import numpy as np

#load paths
path_monthly_magnitude = read_path(input_all_paths, "monthly_magnitude")
path_scopes = read_path(input_all_paths, "scopes")
path_fx = read_path(input_all_paths, "fx")
path_pck_sap = read_path(input_all_paths, "pck_sap")

#read monthly report csv
df_monthly = pd.read_csv(path_monthly_magnitude, parse_dates=["D_PE"], dtype={"D_RU": "str", "T1": "str"})

#consolidate scopes in one file
df_scopes_final = consolidate_scopes(path_scopes)

#add currency to monthly report
df_merged = df_monthly.merge(df_scopes_final[["D_RU", "D_CU", "D_PE"]], how="left", on=["D_RU", "D_PE"])

#read and process FX files
#eliminate budget rows
df_fx=pd.read_excel(path_fx, sheet_name="FX")
index_drop = df_fx[df_fx.Scenario == "Budget"].index
df_fx.drop(index_drop, inplace=True)
df_fx.reset_index(inplace=True, drop=True)

#rename columns
df_fx = df_fx.rename(columns={"FX_RATE_FINAL": "TC", "FX_RATE_AVG": "TMN", "PERIOD": "D_PE", "CURRENCY": "D_CU"})

#eliminate rows with 2019 values
index_drop = df_fx[df_fx.D_PE < "2019-12-01"].index
df_fx.drop(index_drop, inplace=True)
df_fx.reset_index(inplace=True, drop=True)

#get opening FX column
df_fx["TO"] = df_fx["D_CU"].apply(lambda x: get_opening_fx(x,"2020",df_fx))
df_fx.loc[df_fx['D_PE'] < '2020-01-01', 'TO'] = None

#add FX columns and columns with with local currency values 
df_merged = df_merged.merge(df_fx[["TC", "TMN", "D_PE", "D_CU", "TO"]], how="left", on=["D_PE", "D_CU"])
df_merged["FX"] = np.where(df_merged["D_FL"] == "F00", df_merged["TO"], np.where(df_merged["D_FL"] == "F80", 0, df_merged["TMN"]))
df_merged["FLOW_LC"] = df_merged["EUR_Amount"] * df_merged["FX"]
df_merged["CLOSING_FX"] =  df_merged["EUR_Amount"] * df_merged["TC"]

df_pck_sap=pd.read_csv(path_pck_sap, dtype={
    "G/L Account": "str",
    "Order": "str",
    "Account": "str",
    "Aggregate Cost Center": "str",
    "Reversed with": "str",
    "Item": "str"
}, parse_dates=["PE"])

df_pck_sap = df_pck_sap.rename(columns={
    "RU": "D_RU",
    "AC": "D_AC",
    "AU": "D_AU",
    "FL": "D_FL",
    "P_AMOUNT": "FLOW_LC"
    
})

df_pck_sap.loc[:, "PE"] = df_pck_sap.PE.dt.to_period('M').dt.to_timestamp('M')
df_final = pd.concat([df_merged, df_pck_sap])
print("Generating csv...")
df_final.to_csv("monthly_magnitude_pck_sap_2020.csv", index=False)
print("Csv generated.")