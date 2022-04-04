import streamlit as st
import pandas as pd
from datetime import timedelta
import altair as alt
from snowflake.snowpark import functions as F
from dags.snowpark_connection import snowpark_connect
import logging
logging.basicConfig(level=logging.WARN)
logging.getLogger().setLevel(logging.WARN)

session, state_dict = snowpark_connect('./include/state.json')


def update_forecast_table(forecast_df, stations:list, start_date, end_date):
    df = forecast_df.where((F.col('DATE') >= start_date) & 
                           (F.col('DATE') <= end_date))\
                    .select('STATION_ID', 'DATE', 'PRED')\
                    .filter(forecast_df['STATION_ID'].in_(stations))\
                    .to_pandas()
    
    data = df.pivot(index="STATION_ID", columns="DATE", values="PRED")
    st.write("### Weekly Forecast", data)
    
    return None

def update_eval_table(eval_df, stations:list):
    df = eval_df.select('STATION_ID', 'RUN_DATE', 'RMSE')\
                    .filter(eval_df['STATION_ID'].in_(stations))\
                    .to_pandas()

    data = df.pivot(index="STATION_ID", columns="RUN_DATE", values="RMSE")
    st.write("### Model Monitor (RMSE)", data)    
    return None


forecast_df = session.table('FLAT_FORECAST')
eval_df = session.table('FLAT_EVAL')

min_date=session.table('FLAT_FORECAST').select(F.min('DATE')).collect()[0][0]
max_date=session.table('FLAT_FORECAST').select(F.max('DATE')).collect()[0][0]

start_date = st.date_input('Start Date', value=min_date, min_value=min_date, max_value=max_date)
show_days = st.number_input('Number of days to show', value=7, min_value=1, max_value=30)
end_date = start_date+timedelta(days=show_days)

stations_df=session.table('FLAT_FORECAST').select(F.col('STATION_ID')).distinct().to_pandas()

stations = st.multiselect('Choose stations', stations_df['STATION_ID'], ["519", "545"])
if not stations:
    stations = stations_df['STATION_ID']

update_forecast_table(forecast_df, stations, start_date, end_date)

update_eval_table(eval_df, stations)

download_file_names = st.multiselect(label='Monthly ingest file(s):', 
                                     options=['202003-citibike-tripdata.csv.zip'], 
                                     default=['202003-citibike-tripdata.csv.zip'])

st.button('Run Ingest Taskflow', args=(download_file_names))
