import pandas as pd
import os
import re

def read_path(input_all_paths, denomination):
    df = pd.read_excel(input_all_paths, sheet_name="inputs")
    return df[df.denomination==denomination].path.iloc[0]

def consolidate_scopes(path_scopes, path_additional_scopes):
    
    #list scopes folders
    files = os.listdir(path_scopes)
    scope_folder_paths=[]
    
    #iterate over folders and get folder paths
    for f in files:
        path_folder=os.path.join(path_scopes, f)
        if os.path.isdir(path_folder):
            scope_folder_paths.append(path_folder)
    
    scope_files_paths=[]

    for folder in scope_folder_paths:
        files = os.listdir(folder)
        scope_file = [x for x in files if "Scope " in x and ".csv" in x and "M" in x]
        if len(scope_file) > 0:
            scope_files_paths.append(os.path.join(folder, scope_file[0]))
        
    scopes_total=[]

    year="2020"

    for path in scope_files_paths:
        print("path: ", path)

        dtypes = {
            "Reporting unit (code)": "str",
            "Reporting unit (description)": "str",
            'Revised method (Closing)': "str",
            'Revised Conso. (Closing)': "float",
            'Revised Own. Int. (Closing)': "float",
            'Revised Fin. Int. (Closing)': "float",
            "Scope": "str",
            "D_CU": "str"
        }
        try:
            df_scope_month=pd.read_csv(path, dtype=dtypes)
        except:
            df_scope_month=pd.read_csv(path, dtype=dtypes, encoding="utf-16")

        #create column with period values
        month = re.search(r"[0-9]+M", path).group()
        month = re.findall(r"[0-9]+", month)[0]
        date=str(year)+"-"+str(str(month).zfill(2))+"-01"
        df_scope_month["D_PE"] = date
        scopes_total.append(df_scope_month)

    #join all months in one dataframe
    df_scope_total=pd.concat(scopes_total)
    df_scope_total.rename(columns={"Reporting unit (code)": "D_RU"}, inplace=True)
    df_scope_total.loc[:,'D_PE']= pd.to_datetime(df_scope_total['D_PE'])
    
    #return selected columns
    df_scope_total = df_scope_total[[
            'D_RU',
            'Reporting unit (description)',
            'Revised method (Closing)',
            'Revised Conso. (Closing)',
            'Revised Own. Int. (Closing)',
            'Revised Fin. Int. (Closing)',
            'Scope',
            'D_CU',
            'D_PE']]
    
    #getting scopes from additional file
    
    selected_cols = [
        'Reporting unit (code)',
        'Reporting unit (description)',
        'Revised method (Closing)',
        'Revised Conso. (Closing)',
        'Revised Own. Int. (Closing)',
        'Revised Fin. Int. (Closing)'
    ]

    df_additional_scopes=pd.read_excel(path_additional_scopes, sheet_name="Magnitude_Int%", usecols=selected_cols, dtype={'Reporting unit (code)': "str"})
    df_additional_scopes.rename(columns={"Reporting unit (code)": "D_RU"}, inplace=True)
    
    
#     #drop rows where D_RU is present in scopes_consolidated
#     index_drop = df_additional_scopes[df_additional_scopes.D_RU.isin(df_scope_total.D_RU)].index
#     df_additional_scopes.drop(index_drop, inplace=True)
#     df_additional_scopes.reset_index(inplace=True, drop=True)

    #get D_CU
    df_CU =pd.read_excel(path_additional_scopes, sheet_name="COMPANY", usecols=["SIM_CO", "CURRENCY"], dtype={'SIM_CO': "str"})
    df_CU.rename(columns={"SIM_CO": "D_RU", "CURRENCY": "D_CU"}, inplace=True)
    df_additional_scopes = df_additional_scopes.merge(df_CU, on="D_RU", how="left")
    
    #set D_PE
    additional_scopes_list = []
    for i in range(1, 13):
        df_list = df_additional_scopes.copy()
        df_list["D_PE"] = "2020-"+str(i).zfill(2)+"-01"
        additional_scopes_list.append(df_list)
        
    df_final = pd.concat(additional_scopes_list)
    df_final.reset_index(drop=True, inplace=True)
    df_final["Scope"] = "OTHER"
    df_final.loc[:,'D_PE']= pd.to_datetime(df_final['D_PE'])
    
    #add scopes to scopes_consolidated
    max_months = max(list(df_scope_total.D_PE.dt.month.unique()))
    
    for month in range(1, max_months+1):
        period = "2020-"+str(month).zfill(2)+"-01"
        df_final_period=df_final[df_final["D_PE"] == period].copy()
        append_df = df_final_period[df_final_period.D_RU.isin(df_scope_total[df_scope_total.D_PE == period].D_RU)==False]
        df_scope_total = pd.concat([df_scope_total, append_df])
        df_scope_total.reset_index(drop=True, inplace=True)

    return df_scope_total
