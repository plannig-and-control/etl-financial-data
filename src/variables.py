#input paths
input_all_paths="./inputs.xlsx"
year = 2020

#variables for magnitude file
scopes = ["BR", "EDPR-NA", "EDPR ", "NEO-3", "OF"]
year = 2020
path_magnitude = r"C:\Users\E343786\EDP\O365_P&C Data Lake - General\DataSources\Actuals\Magnitude\MAG_TB_AUD"
output_path = r"C:\Users\E343786\EDP\O365_P&C Data Lake - General\DataSources\Actuals\Output"



# path = r"C:\Users\E353952\EDP\O365_P&C Data Lake - General\DataSources\Actuals\Magnitude\MAG_TB_AUD\2020"
# output_path = r"C:\Users\E353952\Desktop\code-projects\ETL-month\output"

#variables for packages and sap file
# path_gl_ru = r'c:\Users\E353952\Desktop\ETL\Bypass\Plano de Contas EDP.csv'
# path_sap = r'C:\Users\E353952\Desktop\ETL\SAP\2020\SAP Source'
# path_sap_csv = r'c:\Users\E353952\Desktop\ETL\SAP\2020\SAP Source CSV'
# path_packages = r'c:\Users\E353952\Desktop\ETL\Packages\2020'
# path_scopes = r"C:\Users\E353952\EDP\O365_P&C Data Lake - General\MetaDataSources\Company\Source\2020"
# path_trading_partner = r'c:\Users\E353952\Desktop\ETL\Join Tables\Trading_partner.xlsx'

scope_equivalences = {
    'EDPR-NA': "EDPR-NA",
    "EDPR-OF": "OF",
    "NEO-3": "NEO-3",
    "GR-EDP-RENOV": "EDPR",
    "EDPR-BR": "BR"
}

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
    "Posting Date": "str", 
    "Reference": "str", 
    "Reversed with": "str",
    "Order": "string",
    "Item": "str",
    "Profit Center": "str"
    }