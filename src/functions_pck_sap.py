import pandas as pd
import os
import numpy as np
import re

##ETL pipeline functions for packages


def read_YTD(path, month):
    '''read package ytd for a certain month
    '''
    folder_month = [f for f in os.listdir(path) if str(month).zfill(2) in f]
    path = os.path.join(path, folder_month[0])
    print(os.listdir(path))
    file_month = [f for f in os.listdir(path) if " "+str(month).zfill(2)+"M" in f][0]
    path_file = os.path.join(path, file_month)
    df = pd.read_csv(path_file, delimiter=";", dtype={"D_RU": "str", "D_ORU": "str", "D_T2": "str"})
    return df


def read_scope(path,month):
    '''read scope excel file for a certain month
    '''
    file = [file for file in os.listdir(path) if " "+str(month)+"M" in file][0]
    path_file = os.path.join(path,file)
    df = pd.read_csv(path_file)
    return df


def transform_df(df):
    '''Input: a dataframe from magnitude in standard format
    Output: the dataframe treated. Ready to be used for further calculations.
    '''
    #Filter initial dataframe
    
    #Clients and products are null
    filter_clients_products = (df["D_CLIENTES"].isnull()) & (df["D_PRODUTOS"].isnull())
    
    #Active, passive and results accounts
    filter_2_1 = df["D_AC"].str.startswith("A")
    filter_2_2 = df["D_AC"].str.startswith("P")
    filter_2_3 = df["D_AC"].str.startswith("R")
    filter_accounts = (filter_2_1 | filter_2_2 | filter_2_3)
    
    #FL values start wuth F, except FA and FB which are dropped
    filter_3 = df["D_FL"].str.startswith("F")
    filter_4 = ~df["D_FL"].str.startswith("FA")
    filter_5 = ~df["D_FL"].str.startswith("FB")
    filter_flow = filter_3 & filter_4 & filter_5
    
    #Other nulls
    filter_6 = df["D_T1"] != "S9999"
    filter_7 = df["D_T2"].isnull()
    filter_8 = df["D_LE"].isnull()
    filter_9 = df["D_NU"].isnull()
    filter_10 = df["D_DEST"].isnull()
    filter_11 = df["D_AREA"].isnull()
    filter_12 = df["D_MU"].isnull()
    filter_13 = df["D_PMU"].isnull()
    filter_other_nulls = filter_6 & filter_7 & filter_8 & filter_9 & filter_10 & filter_11 & filter_12 & filter_13

    #Total filter
    filter_total = filter_clients_products & filter_accounts & filter_flow & filter_other_nulls
    df = df[filter_total]

    ##Drop rows
    #Drop results with F distinct from F99
    filter_1 = (df["D_AC"] == "P8800000") & (df["D_FL"] == "F10")
    filter_2 = ~df["D_AC"].str.startswith('R') & (df["D_FL"] == "F99")
    filter_3 = df["D_AC"].str.startswith('R') & (df["D_FL"] != "F99")
    index_drop = df[filter_1 | filter_2 | filter_3].index
    df = df.drop(index_drop)

    #Drop RU not beggining with "3"
    filter_ru = ~df["D_RU"].str.startswith("3")
    index_drop = df[filter_ru].index
    df = df.drop(index_drop)
    
    #columns selection
    selected_cols = ["D_CA", "D_DP", "D_PE", "D_RU", "D_AC", "D_FL", "D_AU", "D_T1", "P_AMOUNT"]
    df = df[selected_cols]

    #Passive and result multiplied by -1
    df.loc[filter_2_2, "P_AMOUNT"] = df["P_AMOUNT"].multiply(-1)
    df.loc[filter_2_3, "P_AMOUNT"] = df["P_AMOUNT"].multiply(-1)

    #Convert R, F99 to F10
    df.loc[df["D_AC"].str.startswith("R"), 'D_FL'] = "F10"

    ##fix transactions with third parties
    #separated dataframe with blank T1
    index_drop = df[~df["D_T1"].isnull()].index
    df_F_blanks = df.drop(index_drop)
    df_F_blanks.drop(["D_T1"], axis=1, inplace=True)

    #separated dataframe with "group companies" T1
    index_drop = df[df["D_T1"].isnull()].index
    df_F_ggcc = df.drop(index_drop)
    df_F_ggcc.drop(["D_T1"], axis=1, inplace=True)
    df_F_ggcc.loc[:,"P_AMOUNT"] = df_F_ggcc["P_AMOUNT"].multiply(-1)

    #additional dataframe with new calculated "S9999"
    df_third_parties = pd.concat([df_F_blanks, df_F_ggcc])
    print("nulls: ", df_third_parties[["D_CA", "D_DP", "D_PE", "D_RU", "D_AC", "D_FL", "D_AU"]].isnull().sum())
    df_third_parties = df_third_parties.groupby(["D_CA", "D_DP", "D_PE", "D_RU", "D_AC", "D_FL", "D_AU"], as_index=False).sum()
    df_third_parties.loc[:,"D_T1"] = "S9999"
    
    #output dataframe
    index_drop = df[df["D_T1"].isnull()].index
    df = df.drop(index_drop)
    df = pd.concat([df, df_third_parties])

    #type corrections
    df.loc[:,"D_RU"] = df["D_RU"].astype("str")
    df.loc[:,"D_PE"] = df["D_PE"].astype("datetime64[ns]")
    df.loc[:,"D_T1"] = df["D_T1"].astype("str")

    #clear rows with value 0
    df = df[df.P_AMOUNT != 0]
    
    #drop columns
    index_drop = ["D_CA", "D_DP", "D_PE"]
    df.drop(index_drop, axis=1, inplace=True)
    
    #rename columns
    df = df.rename(columns={"D_T1": "T1"})

    return df


def scope_adding(df, df_scopes, scope_equivalences):
    '''given a transformed package dataframe, the consolidated scope dataframe, scope equivalences (to map exactly the scope name that must be input)
    and the period, returns a dataframe with a new column including the scope
    '''
    df_scopes = df_scopes[["D_RU", "Scope", "D_PE"]]
    df = df.merge(df_scopes, on=["D_RU", "D_PE"], how="left")
    df.loc[:,"Scope"] = df["Scope"].map(lambda x: scope_equivalences[x] if x in list(scope_equivalences.keys()) else "OTHER")
    return df

'''given ytd of two consecutive months, it calculates a standalone month
'''
def ytd_to_month(df_YTD_current_month, df_YTD_previous_month):
    df_final_current_month = pd.concat([df_YTD_current_month, df_YTD_previous_month])
    df_final_current_month = df_final_current_month.groupby(['D_RU', 'D_AC', 'D_FL', 'D_AU', 'T1'], as_index=False).sum()
    return df_final_current_month


'''main transforming function for sap dataframes
'''
def transform_sap(df, df_join, df_scopes, scope_equivalences, file_name, max_months):
    
    #Add empty Item column if not found in csv
    
    other_columns = [
        'Aggregate Cost Center',
        'Account',
        'Profit Center',
        'Asset',
        'Flow Type',
        'Vendor',
        'Customer',
        'Item'
        ]

    for column in other_columns:
        if column not in df.columns:
            df[column] = ""

    #columns selection
    selection = [
        'Amount in local currency',
        'Text',
        'Trading partner',
        'G/L Account',
        'Profit Center',
        'Amount in doc. curr.',
        'Order',
        'Year/month',
        'Company Code',
        'WBS element',
        'Purchasing Document',
        'Material',
        'General ledger amount',
        "Assignment",
        "Flow Type",
        "Document Date",
        "Document Number",
        "Document type",
        "User Name",
        'Account',
        "Aggregate Cost Center",
        "Asset",
        "Customer",
        "Vendor",
        "Document currency",
        "Document Header Text",
        "Entry Date",
        "Local Currency",
        "Posting Date",
        "Reference",
        "Reversed with",
        "Item"
        ]
    col_drop = [col for col in df.columns if col not in selection]
    df.drop(col_drop, axis=1, inplace=True)

    #Opening files contain a Posting Date
    print(f"***{file_name}***")
    if "OPENING" not in file_name:
        #format date column. NOTE: except has the format used in Greece SAP file sent on 28/04/2020
        try:
            df.loc[:,"Posting Date"] = pd.to_datetime(df["Posting Date"], format='%Y/%m/%d')
        except:
            df.loc[:,"Posting Date"] = pd.to_datetime(df["Posting Date"], format='%d/%m/%Y')
    else:
        df.loc[:,"Posting Date"] = pd.to_datetime("2020-01-01", format='%Y/%m/%d')

    #correct numbers
    numeric_fields = ['Amount in local currency', 'Amount in doc. curr.', 'General ledger amount']
    df.loc[:, numeric_fields] = df[numeric_fields].replace(",", "", regex=True)
    df.loc[:, numeric_fields] = df[numeric_fields].astype("float")

    #add AU column
    df["D_AU"] = "0LIA01"
    
    #join_df to lookup AC
    df = df.merge(df_join, on="G/L Account", how="left")

    #create D_PE column
    df["D_PE"] = df["Posting Date"].dt.to_period('M').dt.to_timestamp()

    #rename columns
    df.rename(columns={
        "Trading partner": "T1",
        "Company Code": "D_RU",
        "Amount in local currency": "P_AMOUNT"
        }, inplace=True)
    
    #find new columns
    df = df.merge(df_scopes, how="left", on=["D_PE", "D_RU"])
    df = add_t1_cons_col(df, df_scopes)
    df.loc[:, "T1"] = df["T1"].replace("nan", "S9999", regex=True)
    df["T1"].fillna("S9999", inplace = True) 
    df["Flow Type"].fillna("", inplace = True)

    if "OPENING" not in file_name:
        df["D_FL"] = ""
        df["D_FL"] = np.where(df["D_AC"].str.startswith("R"), "F10", df["D_FL"])
        df["D_FL"] = np.where((df["Flow Type"] == "120") & (df["D_FL"] == ""), "F20", df["D_FL"])
        df["D_FL"] = np.where((df["D_AC"].str.startswith(("A42", "A43", "A44")) & (df["Flow Type"] == "") & (df["D_FL"] == "")), "F20", df["D_FL"])
        df["D_FL"] = np.where((df["Flow Type"] == "") & (df["D_FL"] == ""), "F15", df["D_FL"])
        df["D_FL"] = np.where(((df["Flow Type"].str.match(r"^F\d{2}$")) & (df["D_FL"] == "")), df["Flow Type"], df["D_FL"])
        df["D_FL"] = np.where(((~df["Flow Type"].str.match(r"^F\d{2}$")) & (df["D_FL"] == "")), "F15", df["D_FL"])
    else:
        df["D_FL"] = "F00"
    
    df = df.astype({'G/L Account': 'str', "T1": "str", "Order": "str"})
    
    #correct scopes
    df.loc[:,"Scope"] = df["Scope"].map(lambda x: scope_equivalences[x] if x in list(scope_equivalences.keys()) else "OTHER")
    
    if len(list(df[df.D_AC.fillna("").str.startswith("R")].D_FL.unique())) > 1:
        print("_*_*_ WARNING _*_*_", file_name)
    
    if "OPENING" in file_name:
        df.drop(df[df.D_AC.fillna("").str.startswith("R")].index, inplace=True)
        df.reset_index(inplace=True, drop=True)
        print(df[df.D_AC.fillna("").str.startswith("R")])
    return df

def df_codes_gen(path_scopes, month):
    file_scope = [file for file in os.listdir(path_scopes) if " "+str(month)+"M" in file][0]
    df_codes = pd.read_excel(os.path.join(path_scopes, file_scope))
    return df_codes

def df_query_gen(path_gl_ru):
    '''TO-DO: change to link to new query file
    '''
    print(path_gl_ru)
    df_join = pd.read_excel(path_gl_ru, sheet_name="SAP", skiprows=[0], dtype={"Conta": "str", "Conta.1": "str"})
    df_join = df_join.rename(columns={"Conta": "G/L Account", "Conta.1": "D_AC"}) 
    df_join = df_join[["G/L Account", "D_AC"]]
    return df_join

def add_t1_cons_col(df, df_scopes):
    df_scopes = df_scopes.rename(columns={"D_RU": "T1", "Revised method (Closing)": "T1 Revised method (Closing)"})
    merging_columns = ['T1', 'T1 Revised method (Closing)', 'D_PE']
    df = df.merge(df_scopes[merging_columns], on=["T1", "D_PE"], how="left")
    df["T1 Revised method (Closing)"].fillna("External", inplace = True) 
    return df

def sap_dif_mag(df_pck, df_sap):
    #take only certain columns of df_sap and multiply by -1
    df_sap_2 = df_sap[['D_RU', 'D_AC', 'D_FL', 'D_AU', 'T1', 'P_AMOUNT', 'D_SP', 'D_PE']].copy()
    df_sap_2.loc[:,"P_AMOUNT"] = df_sap_2['P_AMOUNT'].multiply(-1)

    #concat and groupby
    df_dif = pd.concat([df_pck, df_sap_2])

    print("packages columns: ", df_pck.columns)
    print("sap columns: ", df_sap_2.columns)
    print("nulls: ", df_sap_2[['D_RU', 'D_AC', 'D_FL', 'D_AU', 'T1', 'D_SP', 'D_PE']].isnull().sum())
    print("check D_AC nulls: ", df_sap.isnull().sum())
    df_dif = df_dif.groupby(['D_RU', 'D_AC', 'D_FL', 'D_AU', 'T1', 'D_SP', 'D_PE'], as_index=False).sum()
    
    df_sap["Source"] = "SAP"
    df_dif["Source"] = "Differences"

    df_final = pd.concat([df_sap, df_dif])

    return df_final

def xlsx_to_csv(input_path, output_path, dtypes_sap):
    files_input = os.listdir(input_path)
    files_output = os.listdir(output_path)
    for files in files_input:
        file_name = str(files[:-4])
        if file_name+"csv" not in files_output:
            print("Converting ", file_name)
            excel_path = os.path.join(input_path, files)
            print(excel_path)
            df = pd.read_excel(excel_path, dtype=dtypes_sap, parse_dates=["Posting Date"])
            print(df["Posting Date"].unique())
            file_name = file_name+"csv"
            df.to_csv(os.path.join(output_path, file_name))
            print(str(file_name)+" created")

