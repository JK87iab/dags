import airflow
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable


# retailer_name  = Variable.get("retailer_name_denver_capacity")
# category  = Variable.get("cat_pipe_denver_capacity")
# version  = Variable.get("ver_pipe_denver_capacity")
# family  = Variable.get("fam_pipe_denver_capacity")

retailer_name  = 'intermountain'
store_group_id = 3000022001123
category  = 'Baseline Forecasting'
version  = 'v1'
family  = 'F1'

args = {
    'owner': 'Jonas',
    'email': ['airflow@example.com'],
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0)  
}

dag = DAG(dag_id='Intermountain-3000022001123', default_args=args,  tags =['Baseline-Forecast', 'Intermountain'])

#
# You can also access the DagRun object in templates

#baseline-denver-500020000113
#define cluster to use
new_cluster = {
    'spark_version': '7.0.x-scala2.12',
    'node_type_id': 'Standard_F16s',
    'driver_node_type_id':'Standard_DS13_v2',
    'num_workers': 7
}

#define cluster to use
new_cluster2 = {
    'spark_version': '7.0.x-scala2.12',
    'node_type_id': 'Standard_F32s_v2',
    'driver_node_type_id':'Standard_DS13_v2',
    'num_workers': 18
}

#First notebook parameter
notebook_task_params = {
    'new_cluster': new_cluster,
    'notebook_task': {'base_parameters':{"retailer_name":retailer_name,"version":version,"categroy":category,"family":family, 'store_group_id': store_group_id},
    'notebook_path': '/Users/jonas.krueger@symphonyretailai.com/CPGAI_modeling/01_read_data',  
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
    'notebook_task': {'base_parameters':{"retailer_name":retailer_name,"version":version,"categroy":category,"family":family,'store_group_id': store_group_id},
    'notebook_path': '/Users/jonas.krueger@symphonyretailai.com/CPGAI_modeling/02_run_train_model',  
  },
})


notebook_task >> notebook_task2

