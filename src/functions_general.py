import pandas as pd
import os
import re

def read_path(input_all_paths, denomination):
    df = pd.read_excel(input_all_paths, sheet_name="inputs")
    return df[df.denomination==denomination].path.iloc[0]

def consolidate_scopes(path_scopes):
    
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
        scope_file = [x for x in files if "Scope " in x and ".xlsx" in x and "M" in x]
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

        df_scope_month=pd.read_excel(path, dtype=dtypes)

        #create column with period values
        month = re.search(r"[0-9]+M", path).group()
        month = re.findall(r"[0-9]+", month)[0]
        date=str(year)+"-"+str(str(month).zfill(2))+"-01"
        df_scope_month["D_PE"] = date
        scopes_total.append(df_scope_month)

    #join all months in one dataframe
    df_scope_total=pd.concat(scopes_total)
    df_scope_total.rename(columns={"Reporting unit (code)": "D_RU"}, inplace=True)
    df_scope_total['D_PE']= pd.to_datetime(df_scope_total['D_PE'])
    
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
    print("check here: ", df_scope_total.columns)
    return df_scope_total