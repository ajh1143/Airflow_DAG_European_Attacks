from __future__ import print_function
import time
from builtins import range
from pprint import pprint
import sys
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import seaborn as sns
from EU_methods import(heatMap, load, count_records, count_columns, list_countries, Belgium, Denmark, France,
                       Germany, Greece, Ireland, Italy, Luxembourg, Netherlands, Portugal, Spain, United_Kingdom)

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}


dag = DAG(
    dag_id='EU_Heat_Map',
    default_args=args,
    schedule_interval=None,
)

#---------------------------#
#           TASKS           #
#---------------------------#


def load_eu(ds, **kwargs):
    load()
    return 'File Loaded'
  
  
def count_record(ds, **kwargs):
    count_records()
    return 'Counted records'
  
  
def count_cols(ds, **kwargs):
    count_columns()
    return 'Extracted/Stored Columns'
  
  
def list_country(ds, **kwargs):
    list_countries()
    return 'Extracted/Stored Countries'
  
  
def Belg(ds, **kwargs):
    Belgium()
    return 'Extracted/Stored Belgium'
  
  
def Den(ds, **kwargs):
    Denmark()
    return 'Extracted/Stored Denmark'
  
  
def Fra(ds, **kwargs):
    France()
    return 'Extracted/Stored France'
  
  
def Ger(ds, **kwargs):
    Germany()
    return 'Extracted/Stored Germany'
def Gre(ds, **kwargs):
    Greece()
    return 'Extracted/Stored Greece'
  
  
def Ire(ds, **kwargs):
    Ireland()
    return 'Extracted/Stored Ireland'
  
  
def Ita(ds, **kwargs):
    Italy()
    return 'Extracted/Stored Italy'
  
  
def Lux(ds, **kwargs):
    Luxembourg()
    return 'Extracted/Stored Luxembourg'
  
  
def Neth(ds, **kwargs):
    Netherlands()
    return 'Extracted/Stored Netherlands'
  
  
def Port(ds, **kwargs):
    Portugal()
    return 'Extracted/Stored Portugal'
  
  
def Spa(ds, **kwargs):
    Spain()
    return 'Extracted/Stored Spain'
  
  
def UK(ds, **kwargs):
    United_Kingdom()
    return 'Extracted/Stored UK'
  
  
def map(ds, **kwargs):
    heatMap()
    return 'Heat Map Generated'

  
#---------------------------#
#         Operators         #
#---------------------------#


a = PythonOperator(
    task_id='Load_Europe_Data_File',
    provide_context=True,
    python_callable=load_eu,
    dag=dag,
)


b = PythonOperator(
    task_id='Count_Records_Raw_File',
    provide_context=True,
    python_callable=count_record,
    dag=dag,
)


c = PythonOperator(
    task_id='List_Countries_Raw',
    provide_context=True,
    python_callable=list_country,
    dag=dag,
)


d = PythonOperator(
    task_id='Belgium',
    provide_context=True,
    python_callable=Belg,
    dag=dag,
)


e = PythonOperator(
    task_id='Denmark',
    provide_context=True,
    python_callable=Den,
    dag=dag,
)


f = PythonOperator(
    task_id='France',
    provide_context=True,
    python_callable=Fra,
    dag=dag,
)


g = PythonOperator(
    task_id='Germany',
    provide_context=True,
    python_callable=Ger,
    dag=dag,
)


h = PythonOperator(
    task_id='Greece',
    provide_context=True,
    python_callable=Gre,
    dag=dag,
)


i = PythonOperator(
    task_id='Ireland',
    provide_context=True,
    python_callable=Ire,
    dag=dag,
)


j = PythonOperator(
    task_id='Italy',
    provide_context=True,
    python_callable=Ita,
    dag=dag,
)


k = PythonOperator(
    task_id='Luxembourg',
    provide_context=True,
    python_callable=Lux,
    dag=dag,
)


l = PythonOperator(
    task_id='Netherlands',
    provide_context=True,
    python_callable=Neth,
    dag=dag,
)


m = PythonOperator(
    task_id='Portugal',
    provide_context=True,
    python_callable=Port,
    dag=dag,
)


n = PythonOperator(
    task_id='Spain',
    provide_context=True,
    python_callable=Spa,
    dag=dag,
)


o = PythonOperator(
    task_id='United_Kingdom',
    provide_context=True,
    python_callable=UK,
    dag=dag,
)


p = PythonOperator(
    task_id='Generate_Heat_Map',
    provide_context=True,
    python_callable=map,
    dag=dag,
)


#---------------------------#
#        Dependencies       #
#---------------------------#

# a = root
a.set_downstream(b)


# b = bottleneck to three threads
b.set_downstream(c)
c.set_downstream([d, e, f, g, h, i, j, k, l, m, n, o])
p.set_upstream([d, e, f, g, h, i, j, k, l, m, n, o])
