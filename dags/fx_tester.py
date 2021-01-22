import requests
import json 
import pandas as pd
from pandas.io.json import json_normalize

api_data_list = []
currency_list = ['GBP', 'INR']
for currency in currency_list:
    api_data = requests.get(
        'https://api.exchangeratesapi.io/latest?base=' + currency.strip()).json()
    api_data_list.append(api_data)
api_df = json_normalize(api_data_list)
api_df.columns = api_df.columns.map(lambda x: x.split(".")[-1])
api_df = api_df.melt(id_vars=["base", "date"], 
        var_name="TARGET_CURRENCY", 
        value_name="FX_RATE_VALUE")
api_df.rename(columns={'base':'SOURCE_CURRENCY','date':'ASOF_DATE'}, inplace=True)
api_df.sort_values(['SOURCE_CURRENCY', 'ASOF_DATE'], inplace=True)
    
