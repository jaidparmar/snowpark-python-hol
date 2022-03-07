
def generate_holiday_df(session, holiday_table_name:str):
    from snowflake.snowpark import functions as F 
    import pandas as pd
    from pandas.tseries.holiday import USFederalHolidayCalendar
    from datetime import timedelta, datetime

    cal = USFederalHolidayCalendar()

    #generate a feature of 20 years worth of US holiday days.
    start_date = datetime.strptime('2013-01-01', '%Y-%m-%d')
    end_date = start_date+timedelta(days=365*20)

    holiday_df = pd.DataFrame(cal.holidays(start=start_date, end=end_date), columns=['DATE'])
    holiday_df['DATE'] = holiday_df['DATE'].dt.strftime('%Y-%m-%d')

    session.create_dataframe(holiday_df) \
           .with_column("HOLIDAY", F.lit(1))\
           .write\
           .save_as_table(holiday_table_name, mode="overwrite", create_temp_table=True)
    
    return session.table(holiday_table_name)

def generate_weather_df(session, weather_table_name):
    from snowflake.snowpark import Window
    from snowflake.snowpark import functions as F 
    import pandas as pd

    tempdf = pd.read_csv('./include/weather.csv')
    tempdf['DATE']=pd.to_datetime(tempdf['dt_iso'].str.replace(' UTC', ''), 
                                 format='%Y-%m-%d %H:%M:%S %z', 
                                 utc=True).dt.tz_convert('America/New_York').dt.date
    tempdf.columns=tempdf.columns.str.upper()
        
    session.create_dataframe(tempdf[['DATE','RAIN_1H', 'TEMP']]) \
           .group_by('DATE').agg([F.round(F.mean('RAIN_1H'), 2).alias('PRECIP'),
                                 F.round(F.mean('TEMP')-F.lit(273.15), 2).alias('TEMP')])\
           .fillna({'PRECIP':0, 'TEMP':0})\
           .write\
           .save_as_table(weather_table_name, mode="overwrite", create_temp_table=True)

    return session.table(weather_table_name)


# def generate_weather_df(session):

#     weather_df = session.table('WEATHERSOURCE_AWS_EU_FRANKFURT_WEATHERSOURCE_FROSTBITE.FROSTBITE.HISTORY_DAY')\
#                        .filter(F.col('POSTAL_CODE') == '10007')\
#                        .select(F.col('DATE_VALID_STD').alias('DATE'), 
#                                F.col('TOT_PRECIPITATION_IN').alias('PRECIP'), 
#                                F.round((F.col('AVG_TEMPERATURE_FEELSLIKE_2M_F')-F.lit(32))*F.lit(5/9), 2).alias('TEMP'))\
#                        .sort('DATE', ascending=True)
#     return weather_df

def generate_features(session, input_df, holiday_table_name, weather_table_name):
    import snowflake.snowpark as snp
    from snowflake.snowpark import functions as F 
    
    #start_date, end_date = input_df.select(F.min('STARTTIME'), F.max('STARTTIME')).collect()[0][0:2]
    
    #check if features are already materialized (or in a temp table)
    holiday_df = session.table(holiday_table_name)
    try: 
        _ = holiday_df.columns()
    except:
        holiday_df = generate_holiday_df(session, holiday_table_name)
        
    weather_df = session.table(weather_table_name)[['DATE','TEMP']]
    try: 
        _ = weather_df.columns()
    except:
        weather_df = generate_weather_df(session, weather_table_name)[['DATE','TEMP']]

    feature_df = input_df.select(F.to_date(F.col('STARTTIME')).alias('DATE'),
                                 F.col('START_STATION_ID').alias('STATION_ID'))\
                         .replace({'NULL': None}, subset=['STATION_ID'])\
                         .group_by(F.col('STATION_ID'), F.col('DATE'))\
                         .count()

    #Impute missing values for lag columns using mean of the previous period.
    mean_1 = round(feature_df.sort('DATE').limit(1).select(F.mean('COUNT')).collect()[0][0])
    mean_7 = round(feature_df.sort('DATE').limit(7).select(F.mean('COUNT')).collect()[0][0])
    mean_365 = round(feature_df.sort('DATE').limit(365).select(F.mean('COUNT')).collect()[0][0])

    date_win = snp.Window.order_by('DATE')

    feature_df = feature_df.with_column('LAG_1', F.lag('COUNT', offset=1, default_value=mean_1) \
                                         .over(date_win)) \
                           .with_column('LAG_7', F.lag('COUNT', offset=7, default_value=mean_7) \
                                         .over(date_win)) \
                           .with_column('LAG_365', F.lag('COUNT', offset=365, default_value=mean_365) \
                                         .over(date_win)) \
                           .join(holiday_df, 'DATE', join_type='left').na.fill({'HOLIDAY':0}) \
                           .join(weather_df, 'DATE', 'inner') \
                          .na.drop() 

    return feature_df
