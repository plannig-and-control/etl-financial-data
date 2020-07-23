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
path_additional_scopes=r"C:\Users\E353952\Desktop\New folder (2)\L_FIN_EDPR_202005_SIM_A_v1.xlsm"

#read monthly report csv
df_monthly = pd.read_csv(path_monthly_magnitude, parse_dates=["D_PE"], dtype={"D_RU": "str", "T1": "str"})

#consolidate scopes in one file
df_scopes_final = consolidate_scopes(path_scopes, path_additional_scopes)

#add currency to monthly report
df_merged_mag = df_monthly.merge(df_scopes_final[["D_RU", "D_CU", "D_PE"]], how="left", on=["D_RU", "D_PE"])

#generate dataframe with fx
df_fx = process_fx(path_fx)

#add FX columns and columns with with local currency values 
df_merged_mag = df_merged_mag.merge(df_fx[["TC", "TMN", "D_PE", "D_CU", "TO"]], how="left", on=["D_PE", "D_CU"])
df_merged_mag["FX"] = np.where(df_merged_mag["D_FL"] == "F00", df_merged_mag["TO"], np.where(df_merged_mag["D_FL"] == "F80", 0, df_merged_mag["TMN"]))
df_merged_mag["FLOW_LC"] = df_merged_mag["EUR_Amount"] * df_merged_mag["FX"]
df_merged_mag["CLOSING_FX"] =  df_merged_mag["EUR_Amount"] * df_merged_mag["TC"]

#reading sap file
df_pck_sap=pd.read_csv(path_pck_sap, dtype=dtypes_sap_transformed, parse_dates=["PE"])

df_pck_sap = df_pck_sap.rename(columns={
    "RU": "D_RU",
    "AC": "D_AC",
    "AU": "D_AU",
    "FL": "D_FL",
    "P_AMOUNT": "FLOW_LC",
    "PE": "D_PE",
    "Scope": "D_SP"
    })

# looking again for D_CU, as there are no values in Source==Differences
# TO-DO volver a chequear las columnas CU_x y CU_y para ver que en Source=SAP son iguales
df_merged_sap = df_pck_sap.merge(df_scopes_final[["D_RU", "D_CU", "D_PE"]], how="left", on=["D_RU", "D_PE"])
df_merged_sap.drop(["D_CU_x"], axis=1, inplace=True)
df_merged_sap.rename(columns={"D_CU_y":"D_CU"}, inplace=True)

#converting P_AMOUNT to euros
df_merged_sap = df_merged_sap.merge(df_fx[["TC", "TMN", "D_PE", "D_CU", "TO"]], how="left", on=["D_PE", "D_CU"])
df_merged_sap["FX"] = np.where(df_merged_sap["D_FL"] == "F00", df_merged_sap["TO"], np.where(df_merged_sap["D_FL"] == "F80", 0, df_merged_sap["TMN"]))
df_merged_sap["EUR_Amount"] = df_merged_sap["FLOW_LC"] / df_merged_sap["FX"]
df_merged_sap["CLOSING_FX"] =  df_merged_sap["FLOW_LC"] / df_merged_mag["TC"]

columns_in_common = ['FLOW_LC', 'T1', 'D_RU', 'D_AU', 'D_AC', 'D_PE', 'D_SP', 'D_FL', 'D_CU', 'EUR_Amount']
grouping_cols = ['T1', 'D_RU', 'D_AU', 'D_AC', 'D_PE', 'D_SP', 'D_FL', 'D_CU']

# TO-DO there cannot be nulls when grouping, check!

df_merged_sap_negative = df_merged_sap.copy()
df_merged_sap_negative.loc[:, "EUR_Amount"] = df_merged_sap_negative["EUR_Amount"].multiply(-1)
df_merged_sap_negative.loc[:, "FLOW_LC"] = df_merged_sap_negative["FLOW_LC"].multiply(-1)

print("columns mag: ", df_merged_mag[columns_in_common].columns)
print("columns sap: ", df_merged_sap_negative[columns_in_common].columns)

df_grouped = pd.concat([df_merged_mag[columns_in_common], df_merged_sap_negative[columns_in_common]])
print("nulls check: ", df_grouped.isnull().sum())
df_grouped.reset_index(drop=True, inplace=True)
df_grouped = df_grouped.groupby(grouping_cols, as_index=False).sum()

df_grouped["Source"] = "Differences Consol"
df_final = pd.concat([df_grouped, df_merged_sap])

col_drop = ["TC", "TMN", "TO", "FX", "CLOSING_FX"]
df_final.drop(col_drop, axis=1, inplace=True)

print("Generating csv...")
df_final.to_csv("../output/monthly_magnitude_pck_sap_2020.csv", index=False)
print("Csv generated.")