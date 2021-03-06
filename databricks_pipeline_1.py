import airflow
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable


retailer_name  = Variable.get("retailer_name_pipe_1")
category  = Variable.get("cat_pipe_1")
version  = Variable.get("ver_pipe_1")
family  = Variable.get("fam_pipe_1")



args = {
    'owner': 'airflow',
    'email': ['airflow@example.com'],
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0)
}

dag = DAG(dag_id='Model_pipe_1', default_args=args)


# You can also access the DagRun object in templates



#define cluster to use
new_cluster = {
    'spark_version': '6.5.x-scala2.11',
    'node_type_id': 'Standard_DS3_v2',
    'num_workers': 1
}

#define cluster to use
new_cluster2 = {
    'spark_version': '6.5.x-scala2.11',
    'node_type_id': 'Standard_F8s',
    'num_workers': 30
}

#First notebook parameter
notebook_task_params = {
    'new_cluster': new_cluster,
    'notebook_task': {'base_parameters':{"retailer_name":retailer_name,"version":version,"categroy":category,"family":family},
    'notebook_path': '/Users/jonas.krueger@symphonyretailai.com/CPGAI_modelling_code/01_read_data',  
  },
}


notebook_task = DatabricksSubmitRunOperator(
  task_id='Read-data-and-build-high-bucket-models',
  dag=dag,
  json=notebook_task_params)


notebook_task2 = DatabricksSubmitRunOperator(
  task_id='Run-high-low-and-final-ranking',
  dag=dag,
  json={
    'new_cluster': new_cluster2,
    'notebook_task': {'base_parameters':{"retailer_name":retailer_name,"version":version,"categroy":category,"family":family},
    'notebook_path': '/Users/jonas.krueger@symphonyretailai.com/CPGAI_modelling_code/02_run_train_model',  
  },
})


notebook_task >> notebook_task2

