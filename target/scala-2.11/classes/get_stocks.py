import pandas as pd
import pandas_datareader as pdr


aapl_dataframe = pdr.get_data_yahoo('AAPL', start = '2017-1-1', end = '2017-1-5')
aapl_dataframe['symbol'] = 'AAPL'
aapl_dataframe['PCT_change'] = 100*(aapl_dataframe['Close']-aapl_dataframe['Open'])/aapl_dataframe['Open']
ge_dataframe = pdr.get_data_yahoo('GE', start = '2017-1-1', end = '2017-1-5')
ge_dataframe['symbol'] = 'GE'
ge_dataframe['PCT_change'] = 100*(ge_dataframe['Close']-ge_dataframe['Open'])/ge_dataframe['Open']
#oil_dataframe = pdr.get_data_yahoo('OIL', start = '2017-1-1', end = '2018-1-1')
#oil_dataframe['symbol'] = 'OIL'
#oil_dataframe['PCT_change'] = 100*(oil_dataframe['Close']-oil_dataframe['Open'])/oil_dataframe['Open']

stk_dataframe = pd.concat([aapl_dataframe, ge_dataframe])
#oil_dataframe])
stk_dataframe.to_csv("src/main/resources/stocks3.xlsx")

