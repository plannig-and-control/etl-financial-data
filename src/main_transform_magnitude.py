import pandas as pd
import os
from variables import *
from functions_general import *
from functions_magnitude import *
import re

def main():
    #load path
    path_magnitude=read_path(input_all_paths, "path_magnitude")

    for folder in os.listdir(path_magnitude):
        period= str(year)+str(folder).zfill(2)+"01"
        #transform files inside folder
        folder_path=os.path.join(path_magnitude, folder)
        files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
        for f in files:
            file_path=os.path.join(folder_path, f)
            df=read_any_YTD(file_path)
            if df.shape[1] == 4:
                df=transform_df(df)
                df["D_SP"] = re.match(r"^\S*", f).group()
                df["SC"] = "Actual"
                df["D_PE"] = pd.to_datetime(period, format="%Y%m%d")
                df.to_csv(file_path[:-4]+"_out.csv", index=False)
        
        #transform files inside LC folder
        folder_path_LC=os.path.join(folder_path, "LC")
        files = [f for f in os.listdir(folder_path_LC) if f.endswith(".csv")]
        for f in files:
            file_path=os.path.join(folder_path_LC, f)
            df=read_any_YTD(file_path)
            print("df shape: ", df.shape[1])
            if df.shape[1] == 4:
                df=transform_df(df)
                df["D_SP"] = re.match(r"^\S*", f).group()
                df["SC"] = "Actual"
                df["D_PE"] = pd.to_datetime(period, format="%Y%m%d")
                print(file_path)
                try:
                    df.to_csv(file_path[:-4]+"_out.csv", index=False)
                except:
                    #load dataframe first to make it available in the hard disk
                    try:
                        df2=read_any_YTD(file_path[:-4]+"_out.csv")
                        df.to_csv(file_path[:-4]+"_out.csv", index=False)
                    except:
                        df2=pd.read_csv(file_path[:-4]+"_out.csv")
                        df.to_csv(file_path[:-4]+"_out.csv", index=False)
if __name__=="__main__":
    print(__name__)
    main()