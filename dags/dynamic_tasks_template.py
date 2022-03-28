
"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
import airflow
import pandas
import numpy as np
import json
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
# from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta
import psycopg2
import configparser

def get_ingestion_object(row):
    table_details_json_str = row.to_json()
    ingestion_json_obj = json.loads("{}")
    ingestion_json_obj["source_info"] = json.loads(table_details_json_str)
    # print(ingestion_json_obj)
    return ingestion_json_obj

def read_ini(file_path):
    config = configparser.ConfigParser()
    config.read(file_path)
    metadb_config = {}
    for section in config.sections():
        for key in config[section]:
            metadb_config[key] = config[section][key]
    return metadb_config

try:
    metadb_config = read_ini("./dags/utils/config.ini")
    server = metadb_config['host']
    port = metadb_config['port']
    database = metadb_config['db']
    username = metadb_config['username']
    password = metadb_config['password']


    connection = psycopg2.connect(user=username,
                                  password=password,
                                  host=server,
                                  port=port,
                                  database=database)
    cursor = connection.cursor()
        
    postgreSQL_select_Query = """
        select batch_name, batch.job_name, job_group, target_table_name,notebook_path,parent_dependencies
        from batch_job_configuration batch
        LEFT JOIN job_configuration job
        ON batch.job_name = job.job_name
        where batch_name='eta_use_case'
    """

    cursor.execute(postgreSQL_select_Query)

    df_ingestion_tables = pandas.read_sql_query(postgreSQL_select_Query, connection)
    # print(df_ingestion_tables.columns)


    args = {
    "owner": "airflow",
    "email": [],
    "email_on_failure": True,
    'email_on_retry': False,
    'databricks_conn_id': 'adb_workspace',
    'start_date': datetime(2021, 10, 25),
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
    }
    dag = DAG("eta_use_case", catchup=False, default_args=args, schedule_interval=None)
    notebook_task_params = {
        'existing_cluster_id': '', # add your cluster id if not dynamic
        'notebook_task': {
            'notebook_path': '',
            "base_parameters": {
                'output_table': '',
                'batch_name': '',
                'run_id': '{{ run_id }}'
            },
            
        },
        'run_name': ''
    }

    # print(conn_json)

    ing_objects = []
    for index, row in df_ingestion_tables.iterrows():
        ing_objects.append(get_ingestion_object(row))
    connection = None
    # print(max_jobs)

    
    if len(ing_objects) > 0:

        # params = json.dumps(ing_objects)
        # # print(type(params))
        # decoded_params = json.loads(params)
        task_operators = {}
        # dim_tables = []
        for src_info in ing_objects:
            # print(table_info['connection'])
            # print(table_info['table'])
            params = json.dumps(src_info)
            job_name = src_info["source_info"]["job_name"]
            t_name = job_name

            notebook_task_params['notebook_task']['notebook_path'] = src_info['source_info']['notebook_path']
            notebook_task_params['notebook_task']['base_parameters']['output_table'] = src_info['source_info']['target_table_name']
            notebook_task_params['notebook_task']['base_parameters']['batch_name'] = src_info['source_info']['batch_name']
            notebook_task_params['run_name'] = f'task_{job_name}'

            notebook_task = DatabricksSubmitRunOperator(
                task_id=f'task_{job_name}',
                dag=dag,
                json=notebook_task_params,
                task_concurrency=1,
                pool='dwh_pool'
                )
            task_operators[t_name] = notebook_task

        for src_info in ing_objects:
            if src_info["source_info"]['parent_dependencies'] != None:
                for j in src_info["source_info"]['parent_dependencies'].split(','):
                    if j ==  None:
                        continue
                    task_name = src_info["source_info"]["job_name"]

                    print(f"{task_operators[task_name]} parent: {task_operators[j]}")
                    task_operators[task_name].set_upstream(task_operators[j])

except (Exception, psycopg2.Error) as error:
    print("Error while fetching data from PostgreSQL", error)

finally:
    # closing database connection.
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")