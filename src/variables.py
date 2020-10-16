#input paths
input_all_paths="./inputs.xlsx"
year = 2020

#variables for magnitude file
scopes = ["BR", "EDPR-NA", "EDPR ", "NEO-3", "OF"]

scope_equivalences = {
    'EDPR-NA': "EDPR-NA",
    "EDPR-OF": "OF",
    "NEO-3": "NEO-3",
    "GR-EDP-RENOV": "EDPR",
    "EDPR-BR": "BR"
}

#magnitude types
dtypes_mag = {
    "D_RU": "category",
    "D_NU": "category",
    "D_LE": "category",
    "D_FL": "category",
    "D_SP": "category",
    "D_CO": "category",
    "D_SC": "category",
    "T1": "category",
    "D_AU": "category",
    "D_AC": "category",
    "D_CU": "category"
}

#pre-transformed sap dtypes
dtypes_sap={
    "G/L Account": "str", 
    "Trading partner": "str",
    "Company Code": "str",
    "Document Date": "str", 
    "Document Number": "str", 
    "Document type": "str", 
    "Account": "str", 
    "User Name": "str",
    "Aggregate Cost Center": "str",
    "Asset": "str",
    "Customer": "str",
    "Vendor": "str",
    "Document currency":"str",
    "Document Header Text": "str", 
    "Local Currency": "str",
    "Reference": "str", 
    "Reversed with": "str",
    "Order": "string",
    "Item": "str",
    "Profit Center": "str",
    "Flow Type": "str"
    }

#post-transformed sap dtypes
dtypes_sap_transformed={
    "G/L Account": "category", 
    "T1": "category",
    "RU": "category",
    "Document Date": "str", 
    "Document Number": "str", 
    "Document type": "str", 
    "AC": "category", 
    "User Name": "str",
    "Aggregate Cost Center": "category",
    "Asset": "category",
    "Customer": "category",
    "Vendor": "str",
    "Document currency":"category",
    "Document Header Text": "str", 
    "Entry Date": "str",
    "Local Currency": "str",
    "Reference": "str", 
    "Reversed with": "str",
    "Order": "category",
    "Item": "category",
    "Profit Center": "category",
    "Scope": "category",
    "Source": "category",
    "D_NU": "category",
    "Reporting unit (description)": "category",
    "FL": "category",
    "AU": "category",
    "User Name": "category",
    "D_CU": "category",
    "Flow Type": "category",
    "Material": "category",
    "Flow Type": "category"
    }

#path to scopes
# path_scopes=r"C:\Users\E353952\EDP\O365_P&C Corporate - 2020"

#path to magnitude monthly csv
# path_monthly_magnitude=r"C:\Users\E353952\EDP\O365_P&C Data Lake - General\DataSources\Actuals\Output\monthly_pl&bs_2020.csv"

#path to pck-sap csv
# path_pck_sap=r"C:\Users\E353952\EDP\O365_P&C Data Lake - General\DataSources\Actuals\Output\monthly_pl&bs_pk&sap_2020.csv"
path_pck_sap = r"..\output\monthly_pl&bs_pk&sap_2020.csv"
#path to FX
# path_fx=r"C:\Users\E353952\EDP\O365_P&C Data Lake - General\MetaDataSources\FX\FX.xlsx"

#output path
# output_path=r"C:\Users\E353952\EDP\O365_P&C Data Lake - General\DataSources\Actuals\Output"