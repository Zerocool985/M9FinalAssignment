#!/home/zeroc/M9Venv/venv/bin/python3
"""
Created on Wed Mar 13 11:52:19 2019

@author: Roman
"""
#import packiges
import pandas as pd
import numpy as np
from fbprophet import Prophet
import os
import timeit
import json
import time
from sqlalchemy import create_engine

#define function for price forecasting
def forecastPrice():
#connect to postgres db
    #with open('C:/Users/Roman/Desktop/Data Science Studium/Modul7_Programming for Data Science/DataScienceProgramming-master/Python/Lecture/configTemplate_RN.json') as f:
    #    conf = json.load(f)
    conn_str ='postgres://postgres:HEJllyxlgKgCLr7l@127.0.0.1:5433/SmartGrid'
    engine = create_engine(conn_str)
#get the required columns from actual price table and rename the columns 
    df1=pd.read_sql_query("select * from stromdaten_tab", engine)
    df1['ds'] = df1['time']
    df1['y'] = df1['act_price']
    df1 = df1.drop(['time', 'act_price', "level_0","index"], axis=1)
#Transform data-column to right data-format for fbprophet
    df1['ds'] = pd.to_datetime(df1['ds'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%Y-%m-%d %H:%M:%S')
    print(df1)

#build und fit time series model    
    model = Prophet()
    model.fit(df1)
    
#make price prediction for next 24 hours
    future1 = model.make_future_dataframe(periods=24*1, freq='H')
    future1.tail
    forecast1 = model.predict(future1)
#   model.plot(forecast1)
#   forc=forecast1.iloc[-24:]
    forc=forecast1[['ds','trend']]
    
#rename columns 
    forc['time'] = forc['ds']
    forc['forc_price'] = forc['trend']
    forc = forc.drop(['ds', 'trend'], axis=1)
    forc['time'] = forc['time'].astype(str)
    print(forc)

#create table in postrges db with forecast price
    forc.to_sql(name='forecast_price_tab',index=True, index_label='index',con=engine, if_exists='append')
    time.sleep(86400)
    
while True:
    forecastPrice()

#####################################################################################

#test postgres values
#test=pd.read_sql_query("SELECT * FROM forecast_price_tab", engine)
#test