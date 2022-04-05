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
#     explainer_columns = [col for col in forecast_df.schema.names if 'EXP' in col]
    explainer_columns=['EXPL_LAG_1', 'EXPL_LAG_7','EXPL_LAG_365','EXPL_HOLIDAY','EXPL_TEMP']
    explainer_columns_new=['DAY', 'DAY_OF_WEEK', 'DAY_OF_YEAR','US_HOLIDAY', 'TEMPERATURE']

    cond = "F.when" + ".when".join(["(F.col('" + c + "') == F.col('EXPLAIN'), F.lit('" + c + "'))" for c in explainer_columns])

    df = forecast_df.filter((forecast_df['STATION_ID'].in_(stations)) &
                       (F.col('DATE') >= start_date) & 
                       (F.col('DATE') <= end_date))\
                .select(['STATION_ID', 
                         F.to_char(F.col('DATE')).alias('DATE'), 
                         'PRED', 
                         'HOLIDAY',
                         *explainer_columns])\
                .with_column('EXPLAIN', F.greatest(*explainer_columns))\
                .with_column('REASON', eval(cond))\
                .select(F.col('STATION_ID'), 
                        F.col('DATE'), 
                        F.col('PRED'), 
                        F.col('REASON'), 
                        F.col('EXPLAIN'), 
                        F.col('EXPL_LAG_1').alias('DAY'),
                        F.col('EXPL_LAG_7').alias('DAY_OF_WEEK'),
                        F.col('EXPL_LAG_365').alias('DAY_OF_YEAR'),
                        F.col('EXPL_HOLIDAY').alias('US_HOLIDAY'),
                        F.col('EXPL_TEMP').alias('TEMPERATURE'),
                       )\
                .to_pandas()
    
    df['REASON'] = pd.Categorical(df['REASON'])
    df['REASON_CODE']=df['REASON'].cat.codes
        
    rect = alt.Chart(df).mark_rect().encode(alt.X('DATE:N'), 
                                        alt.Y('STATION_ID:N'), 
                                        alt.Color('REASON'),
                                        tooltip=explainer_columns_new)
    text = rect.mark_text(baseline='middle').encode(text='PRED:Q', color=alt.value('white'))

    l = alt.layer(
        rect, text
    )

    st.write("### Forecast")
    st.altair_chart(l, use_container_width=True)
        
    return None

def update_eval_table(eval_df, stations:list):
    df = eval_df.select('STATION_ID', F.to_char(F.col('RUN_DATE')).alias('RUN_DATE'), 'RMSE')\
                .filter(eval_df['STATION_ID'].in_(stations))\
                .to_pandas()

    data = df.pivot(index="RUN_DATE", columns="STATION_ID", values="RMSE")
    data = data.reset_index().melt('RUN_DATE', var_name='STATION_ID', value_name='RMSE')

    nearest = alt.selection(type='single', nearest=True, on='mouseover',
                            fields=['RUN_DATE'], empty='none')

    line = alt.Chart(data).mark_line(interpolate='basis').encode(
        x='RUN_DATE:N',
        y='RMSE:Q',
        color='STATION_ID:N'
    )

    selectors = alt.Chart(data).mark_point().encode(
        x='RUN_DATE:N',
        opacity=alt.value(0)
    ).add_selection(
        nearest
    )

    points = line.mark_point().encode(
        opacity=alt.condition(nearest, alt.value(1), alt.value(0))
    )

    text = line.mark_text(align='left', dx=5, dy=-5).encode(
        text=alt.condition(nearest, 'RMSE:Q', alt.value(' '))
    )

    rules = alt.Chart(data).mark_rule(color='gray').encode(
        x='RUN_DATE:N',
    ).transform_filter(
        nearest
    )

    l = alt.layer(
        line, selectors, points, rules, text
    ).properties(
        width=600, height=300
    )
    st.write("### Model Monitor (RMSE)")
    st.altair_chart(l, use_container_width=True)
    
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
