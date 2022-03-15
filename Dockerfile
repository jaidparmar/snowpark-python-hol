#docker build --build-arg PYTHON_MAJOR_MINOR_VERSION=3.8 -t ap-airflow:py38 https://github.com/astronomer/ap-airflow.git#master:2.2.4/bullseye

FROM ap-airflow:py38
COPY include/snowflake_snowpark_python-0.4.1-py3-none-any.whl /tmp
RUN pip install '/tmp/snowflake_snowpark_python-0.4.1-py3-none-any.whl[pandas]'
