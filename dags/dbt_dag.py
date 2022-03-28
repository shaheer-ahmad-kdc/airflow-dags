
"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
import airflow
import pandas
import json
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.bash_operator import BashOperator
# from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta
import psycopg2
from utils.ingestion_queries import *
from utils.helpers import *
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from textwrap import dedent
import copy


def create_dag(dag_id,schedule,default_args, ing_list, load_per_job):
	
	dag = DAG(dag_id,catchup=False, default_args=default_args, schedule_interval=schedule)
	ing_objects = ing_list
	max_jobs = len(ingestion_list)
	with dag:
	   
		for i in range(0, max_jobs, load_per_job):
			params = json.dumps(ing_objects[i:i + load_per_job])
			notebook_task_params['notebook_task']['base_parameters']['ingestion_list'] = params
			notebook_task_params['notebook_task']['base_parameters']['src_connection_type'] = ing_objects[i:i + load_per_job][0]['source']['table']['src_connection_type']

			task_id = ""

			for j in range(0, load_per_job):
				task_id += str(ing_objects[i:i + load_per_job][j]['source']['table']['db_name'])+'_'+ str(ing_objects[i:i + load_per_job][j]['source']['table']['table_name']) 
				task_id = replace_clean_str(task_id).lower()

				notebook_task_params['run_name'] = task_id.lower()
				source = ing_objects[i:i + load_per_job][j]['source']
				targets = ing_objects[i:i + load_per_job][j]['targets']
				struct = ing_objects[i:i + load_per_job][j]['struct']

				with TaskGroup(group_id=task_id) as tg1:
					targetName = 'databricks'
					targetRaw = get_raw_zone_target_props(source,targetName,copy.deepcopy(targets))
					targetRaw = define_target_partition(targetRaw,source['table']['freq'])
					notebook_task_params['notebook_task']['base_parameters']['ingestion_list'] = json.dumps({'source': source, 'target': {targetName : targetRaw}, 'struct': struct})
					notebook_task_params['notebook_task']['base_parameters']['target_type'] = 'raw'

					notebook_task_raw = DatabricksSubmitRunOperator(
						task_id=f"src_to_raw_{targetName}",
						dag=dag,
						json=notebook_task_params,
						task_concurrency=1
					)
					
					for targetName in targets:
						dbt_profile = targetName
						# TODO: call great expectation notebook with expectation params
						notebook_task_great_expectations = DummyOperator(
							task_id=f"great_expectations_{targetName}",
							dag=dag,
							task_concurrency=1
						)
						# print(targetName)
						target = targets[targetName]
						# preparing dbt variables by target
						dbt_vars, target = get_target_props(source,targetName,target)
						target = define_target_partition(target,source['table']['freq'])
						# to use raw as source, "table" props of actual source is required for auditing e.g. ingestion_id etc.
						odsSource = {'connection': targetRaw['connection'], 'table': source['table']}
						# raw will be source to ODS or ODS(STG)
						notebook_task_params['notebook_task']['base_parameters']['ingestion_list'] = json.dumps({'source': odsSource, 'target': {targetName : target}, 'struct': struct})
						


						if spark_ods_engine:
							notebook_task_params['notebook_task']['base_parameters']['target_type'] = 'ods'

							notebook_task_ods = DatabricksSubmitRunOperator(
								task_id=f"raw_to_ods_{targetName}",
								dag=dag,
								json=notebook_task_params,
								task_concurrency=1
							)

							notebook_task_raw >> notebook_task_ods >> notebook_task_great_expectations
						else:
							notebook_task_params['notebook_task']['base_parameters']['target_type'] = 'ods_stg'

							notebook_task_ods_stg = DatabricksSubmitRunOperator(
								task_id=f"raw_to_ods_stg_{targetName}",
								dag=dag,
								json=notebook_task_params,
								task_concurrency=1
							)
							# dbt task
							dbt_refresh = dbt_command_extra(target['table']['delivery_mode'],dbt_vars)
							dbt_commnd = dedent(
								f"""
							source $AIRFLOW_HOME/dbt-env/bin/activate && dbt run --models ods_dynamic_ingestion_model --project-dir $DBT_PROJECT_DIR --profiles-dir $DBT_PROFILES_DIR --profile boltwire_{dbt_profile}_ods --target {DBT_ENVIRONMENT} --vars {dbt_vars} {dbt_refresh}
							"""
							)
							# print(dbt_commnd)
							# print(notebook_task_params)
							dbt_task_ods = BashOperator(
								task_id=f"dbt_merge_ods_{targetName}",
								dag=dag,
								bash_command=dbt_commnd,
								task_concurrency=1
							)
							# TODO: decide where the notebook_task_great_expectations task will be placed from metadata
							notebook_task_raw >> notebook_task_ods_stg >> dbt_task_ods >> notebook_task_great_expectations



	return dag



try:

	metadb_config = read_ini()


	# GETTING METADATA DB PROPERTIES
	server = metadb_config['host']
	port = int(metadb_config['port'])
	database = metadb_config['db']
	username = metadb_config['username']
	password = metadb_config['password']
	jdbc_url = metadb_config['jdbc_url']
	driver = metadb_config['driver']


	spark_ods_engine = json.loads(metadb_config['spark_ods'].lower())
	DBT_ENVIRONMENT = metadb_config['environment'].lower()
	cluster_id =  metadb_config['cluster_id']
	ingestion_notebook = metadb_config['ingestion_notebook']
	 
	connection = psycopg2.connect(user=username,
								  password=password,
								  host=server,
								  port=port,
								  database=database)

	cursor = connection.cursor()
	
	jdbc_ing_query = queries["jdbc_ingestion"]
	jdbc_ing_objects = getIngestionList(connection,jdbc_ing_query)

	# file_ing_query = queries["file_ingestion"]
	# file_ing_objects = getIngestionList(cursor,connection,file_ing_query)

	# ing_objects = [*jdbc_ing_objects, *file_ing_objects]
	ing_objects = [*jdbc_ing_objects]

	ingestion_frequency_queries = queries["ingestion_frequencies"]
	ing_freq_data = pandas.read_sql_query(ingestion_frequency_queries, connection)

	freq_dict = ing_freq_data.to_dict('records')

	ingestion_dict = {}
	for  i in  freq_dict:
		name = i['freq'].lower().replace(" ","_")
		ingestion_dict[name] = {"frequency": i['freq_cron'], "ingest_objects": []}

	for i in ing_objects:
		dag_name = i['source']['table']['freq'].lower().replace(" ","_")
		ingestion_dict[dag_name]["ingest_objects"].append(i)

	args = {
	"owner": "airflow",
	"email": [],
	"email_on_failure": True,
	'email_on_retry': False,
	# 'databricks_conn_id': 'adb_workspace',
	'start_date': datetime(2021, 11, 7),
	'retries': 3,
	'retry_delay': timedelta(minutes=1)
	}

	# dag = DAG("boltwire_daily_ingestion", catchup=False, default_args=args, schedule_interval='@daily')

	notebook_task_params = {
		'existing_cluster_id': cluster_id, # add your cluster id
		'notebook_task': {
			'notebook_path': ingestion_notebook, # absolute path of boltwire_ingestion notebook in databricks workspace
			"base_parameters": {
				'ingestion_list': '',
				'job_id': '{{ run_id }}',
				"audit_parameters":'',
				'src_connection_type': '',
				'target_type': ''
			},
			
		},
		'run_name': ''
	}

	

	audit_params = {
					'audit_jdbc_url' : jdbc_url,
					'audit_conn_prop' : {
							'username': username,
							'password': password,
							'driver': driver
						}
					}
	audit_json =  json.dumps(audit_params)
	notebook_task_params['notebook_task']['base_parameters']['audit_parameters'] = audit_json

	load_per_job = 1

	print("ingestion_dict: ", ingestion_dict)
	exit(0)
	
	for h in ingestion_dict:
		# with DAG(f"bw_{h}_ingestion", catchup=False, default_args=args, schedule_interval=f"{ingestion_dict[h]['frequency']}") as dag:
		
		ingestion_list = ingestion_dict[h]['ingest_objects']
		# max_jobs = len(ingestion_list)


		# globals()[f"bw_{h}_ingestion"] = create_dag(f"bw_{h}_ingestion", f"{ingestion_dict[h]['frequency']}", args, ingestion_list,1)
	

except (Exception, psycopg2.Error) as error:
	print("Error occured: ", error)

# finally:
	# closing database connection.
	# if connection:
	# 	cursor.close()
	# 	connection.close()
	# 	print("PostgreSQL connection is closed")