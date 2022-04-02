import streamlit as st
import pandas as pd
from datetime import timedelta
import altair as alt

#@st.cache
def load_forecast_data(forecast_filename:str):
    df = pd.read_csv(forecast_filename).drop('Unnamed: 0', axis=1)
    df['DATE'] = pd.to_datetime(df['DATE'])
    df['STATION_ID']=df['STATION_ID'].astype(str)    
    return df

#@st.cache
def load_eval_data(eval_filename:str):
    df = pd.read_csv(eval_filename).drop('Unnamed: 0', axis=1)
    df['DATE'] = pd.to_datetime(df['DATE'])
    df['STATION_ID']=df['STATION_ID'].astype(str)    
    return df


def update_forecast_table(forecast_df, stations:list, start_date, end_date):
    forecast_df = forecast_df.loc[(forecast_df['DATE']>=pd.Timestamp(start_date)) & 
                                  (forecast_df['DATE']<pd.Timestamp(end_date))]
    forecast_df['DATE'] = forecast_df['DATE'].dt.strftime('%Y-%m-%d')
    
    data = forecast_df.pivot(index="STATION_ID", columns="DATE", values="PRED").loc[stations]
    st.write("### Weekly Forecast", data)
    
    return None

def update_eval_table(eval_df, stations:list):
    eval_df['DATE'] = eval_df['DATE'].dt.strftime('%Y-%m-%d')
    
    data = eval_df.pivot(index="STATION_ID", columns="DATE", values="RMSE").loc[stations]
    st.write("### Model Monitor (RMSE)", data)    
    return None


forecast_df = load_forecast_data('./forecast_test.csv')
eval_df = load_eval_data('./eval_test1.csv')

min_date=forecast_df['DATE'].min()
max_date=forecast_df['DATE'].max()
max_days=len(forecast_df[forecast_df['STATION_ID'] == forecast_df['STATION_ID'][0]])

start_date = st.date_input('Start Date', value=min_date, min_value=min_date, max_value=max_date)
show_days = st.number_input('Number of days to show', value=7, min_value=1, max_value=max_days)
end_date = start_date+timedelta(days=show_days)

stations = st.multiselect('Choose stations', forecast_df['STATION_ID'].unique(), ["519", "545"])
if not stations:
    stations = forecast_df['STATION_ID'].unique()

update_forecast_table(forecast_df, stations, start_date, end_date)

update_eval_table(eval_df, stations)

download_file_names = st.multiselect(label='Monthly ingest file(s):', 
                                     options=['202003-citibike-tripdata.csv.zip'], 
                                     default=['202003-citibike-tripdata.csv.zip'])

st.button('Run Ingest Taskflow', args=(download_file_names))
