import pandas as pd
import numpy as np
from functions_add_fx import *
from functions_general import *
from variables import *
from playsound import playsound
from datetime import datetime

start_time = datetime.now().strftime("%H:%M:%S")

#generate fx dataframe
path_fx = read_path(input_all_paths, "fx")
df_fx = process_fx(path_fx)

#generate scopes dataframe
path_scopes = read_path(input_all_paths, "scopes")
path_additional_scopes=r"..\input\L_FIN_EDPR_202005_SIM_A_v1.xlsm"
df_scopes_final = consolidate_scopes(path_scopes, path_additional_scopes)

#generate sap part of dataframe
df_pck_sap=pd.read_csv(path_pck_sap, dtype=dtypes_sap_transformed, parse_dates=["PE", "Posting Date"])

rename = {
    "RU": "D_RU",
    "AC": "D_AC",
    "AU": "D_AU",
    "FL": "D_FL",
    "P_AMOUNT": "FLOW_LC",
    "PE": "D_PE",
    "Scope": "D_SP"
    }

df_pck_sap = df_pck_sap.rename(columns=rename)

#apply rename changes to dtypes_sap_transformed
for new_key in rename.keys():
    if new_key in dtypes_sap_transformed.keys():
        dtypes_sap_transformed[rename[new_key]] = dtypes_sap_transformed.pop(new_key)

#Pass Document Date to datetime. Errors=coerce, because there are some null values
df_pck_sap["Document Date"] = pd.to_datetime(df_pck_sap["Document Date"], errors = 'coerce')

# looking again for D_CU, as there are no values in Source==Differences
df_merged_sap = df_pck_sap.merge(df_scopes_final[["D_RU", "D_CU", "D_PE"]], how="left", on=["D_RU", "D_PE"])
df_merged_sap.drop(["D_CU_x"], axis=1, inplace=True)
df_merged_sap.rename(columns={"D_CU_y":"D_CU"}, inplace=True)

#fill na for Document Number and Text
df_merged_sap.loc[(df_merged_sap["Document Number"].isnull()) & (df_merged_sap.Source=="SAP"),"Document Number"] = "-"
df_merged_sap.loc[(df_merged_sap["Text"].isnull()) & (df_merged_sap.Source=="SAP"),"Text"] = "na"

print(df_merged_sap.dtypes)

#create D_LE and D_NU columns
df_merged_sap["D_LE"] = "-"
df_merged_sap["D_NU"] = "-"
df_merged_sap.D_LE = df_merged_sap.D_LE.astype('category')
df_merged_sap.D_NU = df_merged_sap.D_NU.astype('category')


#create magnitude part of dataframe
path_monthly_magnitude = read_path(input_all_paths, "monthly_magnitude")
df_monthly = pd.read_csv(path_monthly_magnitude, parse_dates=["D_PE"], dtype=dtypes_mag)
df_merged_mag = df_monthly.merge(df_scopes_final[["D_RU", "D_CU", "D_PE"]], how="left", on=["D_RU", "D_PE"])
print(df_merged_mag.dtypes)
#fillna for D_LE and D_NU
df_merged_mag['D_LE'] = df_merged_mag['D_LE'].cat.add_categories('-')
df_merged_mag['D_NU'] = df_merged_mag['D_NU'].cat.add_categories('-')
df_merged_mag.D_LE.fillna("-", inplace=True)
df_merged_mag.D_NU.fillna("-", inplace=True)

#check nulls in sap file
col_nulls=["D_RU", "D_AU", "D_AC", "D_PE", "D_SP", "D_FL", "D_CU", "D_LE", "D_NU"]
print("####################################################\n")
print("Check nulls, all should be 0:\n\n", df_merged_sap[col_nulls].isnull().sum())
print("\n####################################################")

#generate dictionary in which each element is accumulated SAP-Dif Consol-Dif Pck with column converted to each month's FX
#previous step for revaluations calculation
dict_monthly = {}
columns_in_common = ['T1', 'D_RU', 'D_AU', 'D_AC', 'D_PE', 'D_SP', 'D_FL', 'D_CU', 'EUR_Amount', "D_LE", "D_NU"]
grouping_cols = ['T1', 'D_RU', 'D_AU', 'D_AC', 'D_PE', 'D_SP', 'D_FL', 'D_CU', "D_LE", "D_NU"]

max_month = max(df_pck_sap.D_PE.dt.month.unique())

print("dtypes sap before processing: /n", df_merged_sap.dtypes)
print("dtypes mag before processing: /n", df_merged_mag.dtypes)

for month in range(1, max_month+1):
    print(f"Converting cummulative month {month} to EUR...")
    
    #filter month
    df_month_sap = df_merged_sap[df_merged_sap.D_PE.dt.month < month+1].copy()
    
    #add FX information and calculate amount in Euros for SAP / Packages info
    df_month_sap = df_month_sap.merge(df_fx[df_fx.D_PE.dt.month==month][["TC", "TMN", "D_CU"]], on=["D_CU"], how="left")
    df_month_sap["FX"] = np.where(df_month_sap["D_AC"].str.startswith("R"), df_month_sap["TMN"], df_month_sap["TC"])
    df_month_sap["EUR_Amount"] = df_month_sap["FLOW_LC"] / df_month_sap["FX"]
    col_drop=["TC", "TMN"]
    df_month_sap.drop(col_drop, axis=1, inplace=True)
    
    #calculate differences to magnitude
    df_mag_month = df_merged_mag[columns_in_common][df_merged_mag.D_PE.dt.month < month+1].copy()
    df_month_sap_negative = df_month_sap[columns_in_common].copy()
    df_month_sap_negative.loc[:, "EUR_Amount"] = df_month_sap_negative.EUR_Amount.multiply(-1)
    df_dif = pd.concat([df_month_sap_negative, df_mag_month], ignore_index=True)
    df_dif = df_dif.groupby(grouping_cols, as_index=False, observed=True).sum()
    df_dif["Source"] = "Differences Consol"
    df_month_sap = pd.concat([df_month_sap, df_dif], ignore_index=True)
    col_category = [key for key in dtypes_sap_transformed.keys() if dtypes_sap_transformed[key] == "category"]
    df_month_sap = col_to_category(df_month_sap, col_category)
    dict_monthly[month] = df_month_sap.copy()

#liberate memory
df_month_sap = None
df_dif = None
df_month_sap_negative = None
df_mag_month = None


#calculating revaluations

#columns for grouping - no SAP part of dataframe
grouping_cols = [
    "T1",
    "D_RU",
    "D_AU",
    "D_AC",
    "D_FL",
    "D_CU",
    "Source",
    "D_SP",
    "D_PE",
    "D_LE",
    "D_NU"
]

#columns for selecting - no SAP part of dataframe
selection_cols = [
    "T1",
    "D_RU",
    "D_AU",
    "D_AC",
    "D_FL",
    "D_CU",
    "EUR_Amount",
    "Source",
    "D_SP",
    "D_PE",
    "D_LE",
    "D_NU"
]

#columns for grouping - SAP part of dataframe
grouping_cols_sap = [
    "T1",
    "D_RU",
    "D_AU",
    "D_AC",
    "D_FL",
    "D_CU",
    "Source",
    "D_SP",
    "D_PE",
    "Document Number",
    "G/L Account",
    "Text",
    "D_LE",
    "D_NU"
]

#columns for selecting - SAP part of dataframe
selection_cols_sap = [
    "T1",
    "D_RU",
    "D_AU",
    "D_AC",
    "D_FL",
    "D_CU",
    "EUR_Amount",
    "Source",
    "D_SP",
    "D_PE",
    "Document Number",
    "G/L Account",
    "Text",
    "D_LE",
    "D_NU"
]
print("***** CHECKING DTYPES IN DICT MONTHLY *****")
for key in dict_monthly.keys():
    print(dict_monthly[key].dtypes)



concat_dfs = []
for month in dict_monthly.keys():
    print(f"Calculating revaluations for month {month}...")
    if month == 1:
        concat_dfs.append(dict_monthly[month].copy())
        print("*** MONTH 1 ***", concat_dfs[0].dtypes)

    else:
        
        #segregate dataframes by sources to calculate revaluations
        
        #segregation of sap part of dataframe
        print("Treating sap part of dataframe...")
        df_prev_sap = dict_monthly[month-1][dict_monthly[month-1].Source=="SAP"].copy()
        df_prev_sap.loc[:, ["EUR_Amount"]] = df_prev_sap["EUR_Amount"].multiply(-1)
        df_dif_sap = pd.concat(
            [dict_monthly[month][selection_cols_sap][(dict_monthly[month].D_PE.dt.month < month) & (dict_monthly[month].Source=="SAP")],
            df_prev_sap[selection_cols_sap]],
            ignore_index=True)
        df_dif_sap = df_dif_sap.groupby(grouping_cols_sap, as_index=False, observed=True).sum()
        df_current_sap = dict_monthly[month][(dict_monthly[month].D_PE.dt.month == month) & (dict_monthly[month].Source=="SAP")].copy()
        df_current_sap["D_RV"] = "Original"
        df_dif_sap["D_RV"] = "Revaluation"
        df_dif_sap["D_PE"] = pd.to_datetime(f"2020-{str(month).zfill(2)}-01")        
        
        #segregation of no sap part of dataframe
        print("Treating non-sap part of dataframe...")
        df_prev = dict_monthly[month-1][dict_monthly[month-1].Source!="SAP"].copy()
        df_prev.loc[:, ["EUR_Amount"]] = df_prev["EUR_Amount"].multiply(-1)
        df_dif = pd.concat([dict_monthly[month][selection_cols][(dict_monthly[month].D_PE.dt.month < month) & (dict_monthly[month].Source!="SAP")], df_prev[selection_cols]])
        df_dif = df_dif.groupby(grouping_cols, as_index=False, observed=True).sum()
        df_current = dict_monthly[month][(dict_monthly[month].D_PE.dt.month == month) & (dict_monthly[month].Source!="SAP")].copy()
        df_current["D_RV"] = "Original"
        df_dif["D_RV"] = "Revaluation"
        df_dif["D_PE"] = pd.to_datetime(f"2020-{str(month).zfill(2)}-01")
        
        #assigning category dtype
        
        df_append = pd.concat([df_current, df_dif, df_current_sap, df_dif_sap], ignore_index=True)
        col_category = [key for key in dtypes_sap_transformed.keys() if dtypes_sap_transformed[key] == "category"]
        df_append = col_to_category(df_append, col_category)
        df_append = df_append[df_append.EUR_Amount != 0].reset_index(drop=True)

        #concat month
        print("Appending generated dataframes together")
        concat_dfs.append(df_append.copy())

        #liberate memory
        if month-2 in dict_monthly.keys():
            dict_monthly[month-2] = None
        
        if month == 2:
            concat_dfs[0]["D_PE"] = pd.to_datetime("2020-01-01")
            concat_dfs[0]["D_RV"] = "Original"

print("Liberating memory setting unused variables to None...")
df_merged_sap = None
df_merged_mag = None
dict_monthly = None
df_prev_sap = None
df_current_sap = None
df_dif_sap = None
df_prev = None
df_current= None
df_dif = None
df_append = None

for i, df in enumerate(concat_dfs):
    # print(f"MONTH {i}: ", df.dtypes)
    print(df.info(memory_usage="deep"))

print("Concat monthly dataframes...")
df_sap_revalued = pd.concat(concat_dfs, ignore_index=True)
df_sap_revalued = col_to_category(df_sap_revalued, col_category)

df_sap_revalued.D_RV = df_sap_revalued.D_RV.astype('category')
# df_sap_revalued.D_PE = df_sap_revalued.D_PE.astype('category')
# df_sap_revalued.D_SP = df_sap_revalued.D_SP.astype('category')
# df_sap_revalued.Source = df_sap_revalued.Source.astype('category')
# df_sap_revalued.D_CU = df_sap_revalued.D_CU.astype('category')
df_sap_revalued.D_LE = df_sap_revalued.D_LE.astype('category')
# df_sap_revalued.T1 = df_sap_revalued.T1.astype('category')
# df_sap_revalued.D_AC = df_sap_revalued.D_AC.astype('category')
# df_sap_revalued.D_RU = df_sap_revalued.D_RU.astype('category')
# df_sap_revalued.D_FL = df_sap_revalued.D_FL.astype('category')

print(df_sap_revalued.info(memory_usage="deep"))
print(df_sap_revalued.memory_usage(deep=True))
print(df_sap_revalued.shape)

#liberate memory
concat_dfs = None

print("Setting selected values D_RV to Original...")
df_sap_revalued.loc[df_sap_revalued.Source=="Differences Consol", "D_RV"] = "Original"
df_sap_revalued.reset_index(inplace=True, drop=True)

print("Dropping zeros...")
#drop 0 values
# index_drop = df_sap_revalued[df_sap_revalued.EUR_Amount == 0].index
# df_sap_revalued.drop(index_drop, inplace=True)
# df_sap_revalued.reset_index(inplace=True, drop=True)

df_sap_revalued = df_sap_revalued[df_sap_revalued.EUR_Amount != 0].reset_index(drop=True)
df_sap_revalued= df_sap_revalued[df_sap_revalued.EUR_Amount.abs() > 0.01].reset_index(drop=True)

#completing profit center column
df_sap_revalued["Profit Center"] = df_sap_revalued["Profit Center"].astype("str")
df_sap_revalued.loc[df_sap_revalued["Profit Center"] == "" , "Profit Center"] = df_sap_revalued.D_RU.astype("str")+"0000"
df_sap_revalued.loc[df_sap_revalued["Profit Center"] == "-" , "Profit Center"] = df_sap_revalued.D_RU.astype("str")+"0000"

#filling GL Account == "-" or "0"
df_sap_revalued["G/L Account"] = df_sap_revalued["G/L Account"].astype("str")
df_sap_revalued.loc[df_sap_revalued["G/L Account"] == "-" , "G/L Account"] = df_sap_revalued.D_AC
df_sap_revalued.loc[df_sap_revalued["G/L Account"] == "0" , "G/L Account"] = df_sap_revalued.D_AC

print("Generating csv...")
df_sap_revalued.rename(columns={"FLOW_LC": "LC_AMOUNT"}, inplace=True)
df_sap_revalued.to_csv("../output/monthly_magnitude_sap_pck_2020.csv", index=False)

print("******** DONE *********")

columns_lite_version = [
    "Text",
    "Amount in doc. curr.",
    "Order",
    "WBS element",
    "Purchasing Document",
    "Material",
    "Assignment",
    "Flow Type",
    "Document Date",
    "Document Number",
    "Document type",
    "User Name",
    "Aggregate Cost Center",
    "Asset",
    "Customer",
    "Vendor",
    "Document currency",
    "Document Header Text",
    "Posting Date",
    "Reference",
    "Reversed with",
    "Item",
    "Reporting unit (description)",
]

df_sap_revalued.drop(columns_lite_version, axis=1, inplace=True)
columns_sap_revalued = list(df_sap_revalued.columns)
grouping_elements = ["LC_AMOUNT", "EUR_Amount"]
for element in grouping_elements:
    if element in list(df_sap_revalued.columns):
        columns_sap_revalued.remove(element)
print("columns before groupby \n", list(df_sap_revalued.columns))
print(columns_sap_revalued)
df_sap_revalued = df_sap_revalued.groupby(columns_sap_revalued, as_index=False, dropna=False, observed=True).sum()
df_sap_revalued.to_csv("../output/monthly_magnitude_sap_pck_2020_lite.csv", index=False)

#create list of Profit Centers
keep = ["D_RU", "Profit Center"]
drop = [col for col in df_sap_revalued.columns if col not in keep]
df_sap_revalued.drop(drop, inplace=True, axis=1)
df_sap_revalued.drop_duplicates(subset=None, keep='first', inplace=True, ignore_index=True)
df_sap_revalued.to_csv("../output/monthly_magnitude_sap_pck_2020_CeCo_list.csv", index=False)


end_time = datetime.now().strftime("%H:%M:%S")
print("Start time: ", start_time, "End time: ", end_time)
playsound("../input/bell_sound.wav")