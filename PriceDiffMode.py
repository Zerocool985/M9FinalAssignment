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

#define function for build price difference and derive mode status 
def difference_and_mode(): 
#connect to postgres db
    #with open('C:/Users/Roman/Desktop/Data Science Studium/Modul7_Programming for Data Science/DataScienceProgramming-master/Python/Lecture/configTemplate_RN.json') as f:
    #    conf = json.load(f)
    conn_str ='postgres://postgres:HEJllyxlgKgCLr7l@127.0.0.1:5433/SmartGrid'
    engine = create_engine(conn_str)
    
#calculate difference between forecasted and actual price and create diff_tab in postgres db
    diff=pd.read_sql_query("select stromdaten_tab.time, forecast_price_tab.time, forecast_price_tab.forc_price, stromdaten_tab.act_price, coalesce(forecast_price_tab.forc_price, 0) - coalesce(stromdaten_tab.act_price, 0) as diff from forecast_price_tab full join stromdaten_tab using (time) where stromdaten_tab.act_price is not null;", engine)
    diff = diff.iloc[-1:] 
    print(diff)
    
    diff.to_sql(name='diff_tab',index=True,con=engine, if_exists='append')

#define mode status and create mode_tab in postgres db   
    mode=pd.read_sql_query("select diff_tab.index, diff_tab.forc_price, diff_tab.act_price, diff_tab.diff, case when diff_tab.diff > 0 then 1 else 0 end as mode from diff_tab;", engine)
    mode = mode.iloc[-1:] 
    print(mode)
    
    mode.to_sql(name='mode_tab',index=True,con=engine, if_exists='append')
    time.sleep(3600)
    
while True:
    difference_and_mode()
#####################################################################################

#test postgres values
#test=pd.read_sql_query("SELECT * FROM mode_tab", engine)
#test


