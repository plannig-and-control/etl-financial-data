import pandas as pd
from functions_general import summary_period
import os

path=r"C:\Users\E353952\EDP\O365_P&C Data Lake - General\DataSources\Actuals\Output\monthly_magnitude_sap_pck_2020_lite.csv"
output_path="../output/"
filename="checks.xlsx"
dst_path = os.path.join(output_path, filename)

pd.set_option('display.float_format', lambda x: '%.2f' % x)

string_cols = ["G/L Account", "D_RU", "Profit Center", "T1", ]
dtypes = dict(zip(string_cols, len(string_cols)*["str"]))
df=pd.read_csv(path, dtype=dtypes)

#Generating dataframes
print("Generating dataframes...")
#APR SUM == 0
df_sum_apr, df_sum_apr_filter = summary_period(df)

#SUM R???? == 0
df_rxxx = df[df["D_AC"].str.fullmatch("R.{4}")]
df_sum_rxxx, df_sum_rxxx_filter = summary_period(df_rxxx, columns=["D_AC", "D_PE", "EUR_Amount"], index=["D_AC"])

#A???? + P???? = 0
df_axxxx_pxxxx = df[df["D_AC"].str.fullmatch("(A.{4})|(P.{4})")]
df_sum_axxxx_pxxxx, df_sum_axxxx_pxxxx_filter = summary_period(df_axxxx_pxxxx, columns=["D_AC", "D_PE", "EUR_Amount"], index=["D_AC"])
filtering_dac_r = [
    'R6833600',
    'R6871000',
    'R6872000',
    'R6873000',
    'R6874000',
    'R6875000',
    'R6876000',
    'R6876100',
    'R6876200',
    'R6876300',
    'R6876900',
    'R6878000',
    'R6879000']

df_dac_r = df[df.D_AC.isin(filtering_dac_r)]
df_f28 = df[df.D_FL=="F28"]
df_r_f28 = pd.concat([df_dac_r, df_f28], ignore_index=True)
df_r_f28.reset_index(inplace=True, drop=True)
df_sum_r_f28, df_sum_r_f28_filter = summary_period(df_r_f28)

# F45=0 by period
df_f45 = df[df.D_FL=="F45"]
df_f45_sum, df_f45_sum_filter = summary_period(df_f45, columns=["D_PE", "EUR_Amount"], index=None)

#PPE Checks
df_ppe_a42 = df[(df["D_AC"].str.fullmatch("A42.{5}")) & (df.D_FL =="F20") & (df.EUR_Amount < 0)]
df_ppe_a43 = df[df["D_AC"].str.fullmatch("A43.{5}") & (df.D_FL =="F30") & (df.EUR_Amount > 0)]
df_ppe_a44 = df[df["D_AC"].str.fullmatch("A44.{5}") & (df.D_FL =="F32") & (df.EUR_Amount > 0)]

#Differences Consol checks
df_dif_cons = df[(df.Source=="Differences Consol") & (df.D_AU =="0LIA01")]
df_sum_dif_cons, df_sum_dif_cons_filter = summary_period(df_dif_cons, columns=["D_SP", "D_PE", "EUR_Amount"], adding_col="EUR_Amount", threshold=10, index=["D_SP"])

#Differences checks
df_dif = df[df.Source=="Differences"]
df_sum_dif, df_sum_dif_filter = summary_period(df_dif_cons, columns=["D_SP", "D_PE", "EUR_Amount"], adding_col="EUR_Amount", threshold=10, index=["D_SP"])

#Generate dictionary with checks

dict_output = {
    "df_sum_apr": df_sum_apr,
    "df_sum_apr_filter": df_sum_apr_filter,
    "df_sum_rxxx": df_sum_rxxx,
    "df_sum_rxxx_filter": df_sum_rxxx_filter,
    "df_sum_axxxx_pxxxx": df_sum_axxxx_pxxxx,
    "df_sum_axxxx_pxxxx_filter": df_sum_axxxx_pxxxx_filter,
    "df_sum_r_f28": df_sum_r_f28,
    "df_sum_r_f28_filter": df_sum_r_f28_filter,
    "df_f45_sum": df_f45_sum,
    "df_f45_sum_filter": df_f45_sum_filter,
    "df_ppe_a42": df_ppe_a42,
    "df_ppe_a43": df_ppe_a43,
    "df_ppe_a44": df_ppe_a44,
    "df_sum_dif_cons": df_sum_dif_cons,
    "df_sum_dif_cons_filter": df_sum_dif_cons_filter,
    "df_sum_dif": df_sum_dif,
    "df_sum_dif_filter": df_sum_dif_filter
}

print("Generating checks excel file...")
with pd.ExcelWriter(dst_path) as writer:
    for key in dict_output.keys():
        df = dict_output[key]
        df.to_excel(writer, sheet_name=key)