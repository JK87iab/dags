import airflow
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import Variable


# retailer_name  = Variable.get("retailer_name_denver_capacity")
# category  = Variable.get("cat_pipe_denver_capacity")
# version  = Variable.get("ver_pipe_denver_capacity")
# family  = Variable.get("fam_pipe_denver_capacity")

# retailer_name  = 'portland'
# store_group_id = 1900020001183
# category  = 'Baseline Forecasting'
# version  = 'v1'
# family  = 'F1'

args = {
    'owner': 'Jonas',
    'email': ['airflow@example.com'],
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0)  
}

dag = DAG(dag_id='INTERMOUNTAIN All', default_args=args,  tags =['Baseline-Forecast', 'Intermountain'])

#
# You can also access the DagRun object in templates


subdag0 = SubDagOperator(
        task_id='Intermountain-3000022001121',
        subdag=subdag(Intermountain-3000022001121),
        dag=dag,
        )


subdag1 = SubDagOperator(
        task_id='Intermountain-3000022001123',
        subdag=subdag(Intermountain-3000022001123),
        dag=dag,
        )

subdag2 = SubDagOperator(
        task_id='Intermountain-3000022001122',
        subdag=subdag(Intermountain-3000022001122),
        dag=dag,
        )


subdag3 = SubDagOperator(
        task_id='Intermountain-3000020001122',
        subdag=subdag(Intermountain-3000020001122),
        dag=dag,
        )


subdag4 = SubDagOperator(
        task_id='Intermountain-3000020001123',
        subdag=subdag(Intermountain-3000020001123),
        dag=dag,
        )

subdag5 = SubDagOperator(
        task_id='Intermountain-3000020001121',
        subdag=subdag(Intermountain-3000020001121),
        dag=dag,
        )

subdag0 >> subdag1 >> subdag2 >> subdag3 >> subdag4 >> subdag5 