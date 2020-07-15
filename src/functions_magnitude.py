import pandas as pd
from variables import *
import os


def read_YTD(path, month, scope):
    '''
    Given the path, the selected month and the selected scope, the function returns a dataframe
    with the csv selected. The structure of folders in the path should be by months
    '''
    print(f'**Reading {month} and scope {scope}**')
    folder = [folder for folder in os.listdir(path) if str(month).zfill(2) in folder][0]
    file_csv = [file_csv for file_csv in os.listdir(os.path.join(path,folder)) if scope in file_csv][0]
    path_file = os.path.join(path,folder,file_csv)
    print("path read: ", path_file)
    df = pd.read_csv(path_file, dtype={
        "RU": "str",
        "AC": "str",
        "AU": "str",
        "FL": "str",
        "D_LE": "str",
        "D_NU": "str",
        "D_SP": "str",
    })
    #here we do try except to catch files encoded in utf-8 if there are any
    # try:
    #     df = pd.read_csv(path_file, encoding="utf-16")
    #     if df.shape[1] != 4:
    #         df = pd.read_csv(path_file, encoding="utf-16", delimiter=";")
    # except:
    #     df = pd.read_csv(path_file, encoding="utf-8")
    #     if df.shape[1] != 4:
    #         df = pd.read_csv(path_file, encoding="utf-8", delimiter=";")
    
    print(f'**DataFrame for month {month} and scope {scope} succesfully read**')
    df=df.astype({"D_NU": "str", "D_LE": "str"})
    return df

def read_any_YTD(path):
    '''
    Given the path, the selected month and the selected scope, the function returns a dataframe
    with the csv selected. The structure of folders in the path should be by months
    '''
    print(f'**Reading {path} **')
    
    #here we do try except to catch files encoded in utf-8 if there are any
    try:
        df = pd.read_csv(path, encoding="utf-16")
        if df.shape[1] != 4:
            df = pd.read_csv(path, encoding="utf-16", delimiter=";")
    except:
        df = pd.read_csv(path, encoding="utf-8")
        if df.shape[1] != 4:
            df = pd.read_csv(path, encoding="utf-8", delimiter=";")
    
    print(f'**DataFrame {path} read**')
    return df

def transform_df(df):
    '''
    Input: a dataframe from magnitude in standard format
    Output: the dataframe treated. Ready to be used for further calculations.
    '''

    #eliminate first row which is null
    df.drop([0], inplace=True)
    df.drop(["Unnamed: 1", "Unnamed: 3"], axis=1, inplace=True)
    print("Transformation initiated.")

    #eliminate columns with no values
    df.columns.values[1] = "VL"
    if (df["VL"].dtype == "object"):
        df.loc[:,"VL"] = df["VL"].str.replace("-", "0").astype(float)

    #separate columns by delimiter
    df[["RU", "AC", "FL", "T1", "AU", "D_LE", "D_NU"]] = df["Unnamed: 0"].str.split(pat = "/", expand=True)
    df.drop(["Unnamed: 0"], axis=1, inplace=True)

    #filter dataframe
    filter_1 = df["AC"].str.startswith('R')
    filter_2 = (df["AC"] == "P8800000") & (df["FL"] == "F10")
    filter_3 = ~df["AC"].str.startswith('R') & (df["FL"] == "F99")
    filter_4 = df["AC"].str.startswith('R') & (df["FL"] != "F99")
    filter_5 = df["AC"].str.startswith('P')
    index_drop = df[filter_2 | filter_3 | filter_4].index
    df = df.drop(index_drop)
    reordered_cols = ['RU', 'AC', 'T1', 'AU', 'FL', 'VL', 'D_LE', 'D_NU']
    df = df[reordered_cols]
    df.loc[:,"VL"] = df["VL"].div(100)
    df.loc[filter_1 | filter_5, 'VL'] = df["VL"].multiply(-1)
    df.loc[df["AC"].str.startswith("R"), 'FL'] = "F10"
    print("DataFrame filtered")

    ##fix transactions with third parties
    #delete third parties
    index_drop = df[df["T1"] == "S9999"].index
    df = df.drop(index_drop)
    print("S9999 dropped")

    #separated dataframe with blank T1
    index_drop = df[df["T1"] != ""].index
    df_F_blanks = df.drop(index_drop)
    df_F_blanks.drop(["T1"], axis=1, inplace=True)
    print("T1 blank generated")

    #separated dataframe with "group companies" T1
    index_drop = df[df["T1"] == ""].index
    df_F_ggcc = df.drop(index_drop)
    df_F_ggcc.drop(["T1"], axis=1, inplace=True)
    df_F_ggcc.loc[:,"VL"] = df_F_ggcc["VL"].div(-1)
    print("T1 with group companies generated")

    #additional dataframe with new calculated "S9999"
    df_third_parties = pd.concat([df_F_blanks, df_F_ggcc])
    print("Concat successful")
    df_third_parties = df_third_parties.groupby(["RU", "AC", "AU", "FL", "D_LE", "D_NU"], as_index=False).sum()
    print("GroupBy successful")
    df_third_parties["T1"] = "S9999"
    print("Third parties fixed")

    #output dataframe
    index_drop = df[df["T1"] == ""].index
    df = df.drop(index_drop)
    df = pd.concat([df, df_third_parties])
    print("Transformation finalized")
    
    return df

def df_add_to_list(list_df_individual, list_df_consolidated, transformed_df, scope, period):
    transformed_df["PE"] = pd.to_datetime(period, format='%Y%m%d')
    transformed_df["Scope"] = scope
    if scope != "EDPR ":
        list_df_individual.append(transformed_df)
    else:
        list_df_consolidated.append(transformed_df)
    print("Balance in this dataframe: ", transformed_df.VL.sum())

def ytd_to_month(df_YTD_current_month, df_YTD_previous_month):
    '''
    Calculates month from two year-to-dates dataframe. The "previous" values must be previously multiplied by -1.
    '''
    df_final_current_month = pd.concat([df_YTD_current_month, df_YTD_previous_month])
    df_final_current_month = df_final_current_month.groupby(["RU", "AC", "T1", "AU", "FL", "D_LE", "D_NU"], as_index=False).sum()
    return df_final_current_month

def bridge_ind_cons(list_df_individual, list_df_consolidated, list_df_month):
    '''
    Takes monthly dataframes from individual scopes, and EDPR monthly consolidated dataframes and calculates differences.
    Does not return anything, as it just appends elements to lists.
    '''
    df = pd.concat(list_df_individual)
    df = df.loc[:,['RU', 'AC', 'T1', 'AU', 'FL', 'VL', 'PE', "D_LE", "D_NU"]]
    df = df.groupby(['RU', 'AC', 'T1', 'AU', 'FL', 'PE', "D_LE", "D_NU"], as_index=False).sum()
    df.loc[:,"VL"] = df["VL"].multiply(-1)
    
    df_consolidated = pd.concat(list_df_consolidated)
    df_consolidated=df_consolidated.astype({"D_NU": "str", "D_LE": "str"})
    df_consolidated = df_consolidated.loc[:,['RU', 'AC', 'T1', 'AU', 'FL', 'VL', 'PE', "D_LE", "D_NU"]]
    
    df_dif = pd.concat([df, df_consolidated])
    df_dif=df_dif.astype({"D_NU": "str", "D_LE": "str"})
    df_dif = df_dif.groupby(['RU', 'AC', 'T1', 'AU', 'FL', 'PE', "D_LE", "D_NU"], as_index=False).sum()
    
    df_consolidated["Scope"] = "EDPR"
    df_dif["Scope"] = "CH"
    
    list_df_individual.extend([df_dif])
    df_final = pd.concat(list_df_individual)
    list_df_month.append(df_final)