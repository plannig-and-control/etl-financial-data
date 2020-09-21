import pandas as pd
import os
from functions_magnitude import *
from functions_general import *
from variables import *
from playsound import playsound
'''
TO-DO create a log report to check if all files are in folders
'''
def main():
    #load paths
    path_magnitude=read_path(input_all_paths, "path_magnitude")
    output_path=read_path(input_all_paths, "monthly_magnitude")
    print(output_path)
    # output_path= "../output/"

    #checking in folder how many months can be processed
    max_months = max([int(x) for x in os.listdir(path_magnitude)])

    #initializing auxiliar lists and dictionaries
    list_df_months = []
    list_df_individual = []
    list_df_consolidated = []
    dict_transformed_previous_month = {}

    #iterating over months and scopes
    for month in range(1, max_months+1):    
        
        period = str(year)+str(month).zfill(2)+"01"

        if month == 1:
            for scope in scopes:
                df_current_month = read_YTD(path_magnitude, month, scope)
                col_drop=["D_SP", "SC", "D_PE"]
                df_current_month.drop(col_drop, axis=1, inplace=True)
                df_add_to_list(list_df_individual, list_df_consolidated, df_current_month, scope, period)
                dict_transformed_previous_month[scope] = df_current_month.copy()
        else:
            for scope in scopes:
                for i in range(max(month-1,1),month+1):
                    if i == month:
                        df_current_month = read_YTD(path_magnitude, i, scope)
                        df_current_month.drop(col_drop, axis=1, inplace=True)
                    else:
                        df_previous_month = dict_transformed_previous_month[scope]
                        df_previous_month.loc[:,"VL"] = df_previous_month["VL"].multiply(-1)
                df = ytd_to_month(df_current_month, df_previous_month)
                dict_transformed_previous_month[scope] = df_current_month.copy()
                df_add_to_list(list_df_individual, list_df_consolidated, df, scope, period)
                print(f'**{scope} for month {month} correctly processed**')
    
    #calculating differences vs consolidated
    bridge_ind_cons(list_df_individual, list_df_consolidated, list_df_months)
    df_final = pd.concat(list_df_months)

    #applying final transformations
    df_final.loc[:,"VL"] = df_final["VL"].round(2)
    df_final["CO"] = "Consolidated"
    df_final["SC"] = "Actual"
    df_final = df_final.add_prefix("D_")
    df_final = df_final.rename(columns={"D_T1": "T1", "D_VL": "EUR_Amount", "D_Scope": "D_SP", "D_D_LE": "D_LE", "D_D_NU": "D_NU"})
    df_final.loc[:, "D_PE"] = df_final.D_PE.astype("str")
    print(df_final.D_PE.unique())
    df_final = df_final[df_final["EUR_Amount"] != 0]
    print("Generating CSV...")
    file_name = f"monthly_pl&bs_{year}.csv"
    df_final.to_csv(output_path, index=False)
    print("CSV successfully generated")

if __name__ == "__main__":
    main()
    playsound("../input/bell_sound.wav")