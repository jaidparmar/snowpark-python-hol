
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from dags.airflow_tasks import snowpark_database_setup
from dags.airflow_tasks import incremental_elt_task
from dags.airflow_tasks import initial_bulk_load_task
from dags.airflow_tasks import materialize_holiday_task
from dags.airflow_tasks import materialize_weather_task
from dags.airflow_tasks import deploy_model_udf_task
from dags.airflow_tasks import deploy_eval_udf_task
from dags.airflow_tasks import generate_feature_table_task
from dags.airflow_tasks import generate_forecast_table_task
from dags.airflow_tasks import bulk_train_predict_task
from dags.airflow_tasks import eval_station_models_task 
from dags.airflow_tasks import flatten_tables_task

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

#local_airflow_path = '/usr/local/airflow/'

@dag(default_args=default_args, schedule_interval=None, start_date=datetime(2022, 3, 14), catchup=False, tags=['setup'])
def citibikeml_setup_taskflow(s3_bucket, s3_role_arn):
    """
    Setup initial Snowpark / Astronomer ML Demo
    """
    import uuid
    import json
    
    with open('./include/state.json') as sdf:
        state_dict = json.load(sdf)
        
    model_id = str(uuid.uuid1()).replace('-', '_')

    state_dict.update({'model_id': model_id})
    state_dict.update({'download_base_url': 'https://s3.amazonaws.com/tripdata/',
                       'load_table_name': 'RAW_',
                       'trips_table_name': 'TRIPS',
                       'load_stage_name': 'LOAD_STAGE',
                       'model_stage_name': 'MODEL_STAGE',
                       'weather_table_name': 'WEATHER',
                       'holiday_table_name': 'HOLIDAYS',
                       'clone_table_name': 'CLONE_'+model_id,
                       'feature_table_name' : 'FEATURE_'+model_id,
                       'pred_table_name': 'PRED_'+model_id,
                       'eval_table_name': 'EVAL_'+model_id,
                       'forecast_table_name': 'FORECAST_'+model_id,
                       'forecast_steps': 30,
                       'train_udf_name': 'station_train_predict_udf',
                       'train_func_name': 'station_train_predict_func',
                       'eval_udf_name': 'eval_model_output_udf',
                       'eval_func_name': 'eval_model_func'
                      })


    setup_state_dict = snowpark_database_setup(state_dict)
    load_state_dict = initial_bulk_load_task(setup_state_dict, download_base_url, download_role_ARN)
    holiday_state_dict = materialize_holiday_task(setup_state_dict)
    weather_state_dict = materialize_weather_task(setup_state_dict)
    model_udf_state_dict = deploy_model_udf_task(setup_state_dict)
    eval_udf_state_dict = deploy_eval_udf_task(setup_state_dict)
    feature_state_dict = generate_feature_table_task(load_state_dict, holiday_state_dict, weather_state_dict) 
    foecast_state_dict = generate_forecast_table_task(load_state_dict, holiday_state_dict, weather_state_dict)
    pred_state_dict = bulk_train_predict_task(model_udf_state_dict, feature_state_dict, foecast_state_dict)
    eval_state_dict = eval_station_models_task(eval_udf_state_dict, pred_state_dict)  
    state_dict = flatten_tables_task(pred_state_dict, eval_state_dict)

    return state_dict

download_role_ARN='arn:aws:iam::484577546576:role/citibike-demo-ml-s3-role'
download_base_url='s3://citibike-demo-ml/data/'
state_dict = citibikeml_setup_taskflow(download_base_url, download_role_ARN)
