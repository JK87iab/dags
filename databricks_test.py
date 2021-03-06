import airflow
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

args = {
    'owner': 'airflow',
    'email': ['airflow@example.com'],
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0)
}

dag = DAG(dag_id='example_databricks_operator', default_args=args)


# You can also access the DagRun object in templates
bash_task = BashOperator(
    task_id="bash_task",
    bash_command='echo "Here is the messagem: '
                 '{{ dag_run.conf if dag_run else "" }}" ',
    dag=dag,
)


#define cluster to use
new_cluster = {
    'spark_version': '6.5.x-scala2.11',
    'node_type_id': 'Standard_DS3_v2',
    'num_workers': 1
}

#First notebook parameter
notebook_task_params = {
    'new_cluster': new_cluster,
    'notebook_task': {'base_parameters':{"param1":"xyz","param2":"123"},
    'notebook_path': '/Users/jonas.krueger@symphonyretailai.com/airflow/airflow_test',  
  },
}

# notebook_task_params2 = {
#     'new_cluster': new_cluster,
#     'notebook_task': {'base_parameters':{"retailer_name":context['dag_run'].conf.get('retailer_name')}, 
#     'notebook_path': '/Users/jonas.krueger@symphonyretailai.com/airflow/airflow_test_2',  
#   },
# }

notebook_task = DatabricksSubmitRunOperator(
  task_id='Run-notebook-1',
  dag=dag,
  json=notebook_task_params)


notebook_task2 = DatabricksSubmitRunOperator(
  task_id='Run-notebook-2',
  dag=dag,
  json={
    'new_cluster': new_cluster,
    'notebook_task': {'base_parameters':{"retailer_name": '{{ dag_run.conf["retailer_name"] if dag_run else "" }}' ,
                                        "cat": '{{ dag_run.conf["cat"] if dag_run else "" }}',
                                        "fam": '{{ dag_run.conf["fam"] if dag_run else "" }}'},
    'notebook_path': '/Users/jonas.krueger@symphonyretailai.com/airflow/airflow_test_2',  
  },
})


bash_task >> notebook_task >> notebook_task2

#curl -n https://adb-5527455231214377.17.azuredatabricks.net/api/2.0/clusters/spark-versions
#{"retailer_name":"albertson" ,"cat":"CIG Level", "fam":"F3"}