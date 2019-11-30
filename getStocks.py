import pandas as pd
import pandas_datareader as pdr
import json

with open("my_script_conf.json") as conf:
	json_obj = json.load(conf)
	start_date = json_obj["time"]["start"]
	stop_date = json_obj["time"]["end"]

stk_dataframes = []
with open("symbols.txt") as symbolfile:
	for symbol in symbolfile:
		temp_dataframe = pdr.get_data_yahoo(symbol.strip(), start = start_date, end = stop_date)
		temp_dataframe['symbol'] = symbol.strip()
		stk_dataframes.append(temp_dataframe)

stk_dataframe = pd.concat(stk_dataframes)
stk_dataframe['PCT_change'] = 100*(stk_dataframe['Close']-stk_dataframe['Open'])/stk_dataframe['Open']
stk_dataframe.to_csv("src/main/resources/stocks.xlsx")

