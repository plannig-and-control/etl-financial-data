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
    "Entry Date": "str",
    "Local Currency": "str",
    # "Posting Date": "str", 
    "Reference": "str", 
    "Reversed with": "str",
    "Order": "string",
    "Item": "str",
    "Profit Center": "str"
    }

#post-transformed sap dtypes
dtypes_sap_transformed={
    "G/L Account": "str", 
    "T1": "str",
    "RU": "str",
    "Document Date": "str", 
    "Document Number": "str", 
    "Document type": "str", 
    "AC": "str", 
    "User Name": "str",
    "Aggregate Cost Center": "str",
    "Asset": "str",
    "Customer": "str",
    "Vendor": "str",
    "Document currency":"str",
    "Document Header Text": "str", 
    "Entry Date": "str",
    "Local Currency": "str",
    "Posting Date": "str", 
    "Reference": "str", 
    "Reversed with": "str",
    "Order": "string",
    "Item": "str",
    "Profit Center": "str"
    }

#path to scopes
path_scopes=r"C:\Users\E353952\EDP\O365_P&C Corporate - 2020"

#path to magnitude monthly csv
path_monthly_magnitude=r"C:\Users\E353952\EDP\O365_P&C Data Lake - General\DataSources\Actuals\Output\monthly_pl&bs_2020.csv"

#path to pck-sap csv
path_pck_sap=r"C:\Users\E353952\EDP\O365_P&C Data Lake - General\DataSources\Actuals\Output\monthly_pl&bs_pk&sap_2020.csv"

#path to FX
path_fx=r"C:\Users\E353952\EDP\O365_P&C Data Lake - General\MetaDataSources\FX\FX.xlsx"

#output path
output_path=r"C:\Users\E353952\EDP\O365_P&C Data Lake - General\DataSources\Actuals\Output"