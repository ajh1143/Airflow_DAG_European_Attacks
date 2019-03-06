# Airflow DAG     
## Generate a Heat Map of terror attacks in European countries from 1970-2014 through Apache Airflow

## Output
<img src="https://github.com/ajh1143/Airflow_DAG_European_Attacks/blob/master/Figure_1.png" class="inline"/><br>


## DAG Graph View
<img src="https://github.com/ajh1143/Airflow_DAG_European_Attacks/blob/master/Graph_View.png" class="inline"/><br>

## DAG File
### Imports
```Python3
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
```
### DAG Config
```Python3
args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}


dag = DAG(
    dag_id='EU_Heat_Map',
    default_args=args,
    schedule_interval=None,
)
```
### Tasks
```Python3
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
```
### Operators
```Python3
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
```
### Dependency Mapping
```Python3
#---------------------------#
#        Dependencies       #
#---------------------------#

# a = root
a.set_downstream(b)


# b = bottleneck to three threads
b.set_downstream(c)
c.set_downstream([d, e, f, g, h, i, j, k, l, m, n, o])
p.set_upstream([d, e, f, g, h, i, j, k, l, m, n, o])
```

## Method File 
### Imports
```Python3
import pandas as pd
import numpy as np
import os
import sys
import time
import seaborn as sns
import os
import matplotlib.pyplot as plt
plt.switch_backend('agg')
location_list = []
```

### Load File
```Python3
def load():
    file_source = r"/root/airflow/dags/eu_terrorism_fatalities_by_country.csv"
    df = pd.read_csv(file_source)
    file_destination = r"/root/airflow/dags/EU_Folder/EU_DataFrame.csv"
    df.to_csv(file_destination)
```

### Count Total Records in Dataset
```Python3
def count_records():
    file_source = r"/root/airflow/dags/EU_Folder/EU_DataFrame.csv"
    df = pd.read_csv(file_source)
    print("Collected {} records".format(len(df)))
```

### Count Countries Included
```Python3
def count_columns():
    file_source = r"/root/airflow/dags/EU_Folder/EU_DataFrame.csv"
    df = pd.read_csv(file_source)
    print("Contains: {} columns".format(len(df.columns)))
```

### View Countries
```Python3
def list_countries():
    file_source = r"/root/airflow/dags/EU_Folder/EU_DataFrame.csv"
    df = pd.read_csv(file_source)
    print("Countries: {}".format(list(df.columns)))
```

### ETL - Slice Countries, Build DataFrame, Generate Individual Files
```Python3
def Belgium():
    global location_list
    file_source = r"/root/airflow/dags/EU_Folder/EU_DataFrame.csv"
    location_list.append(file_source)
    df = pd.read_csv(file_source)
    df_belgium = df['Belgium']
    file_destination = r"/root/airflow/dags/EU_Folder/Belgium.csv"
    df_belgium.to_csv(file_destination,  index=False)


def Denmark():
    global location_list
    file_source = r"/root/airflow/dags/EU_Folder/EU_DataFrame.csv"
    location_list.append(file_source)
    df = pd.read_csv(file_source)
    df = df['Denmark']
    file_destination = r"/root/airflow/dags/EU_Folder/Denmark.csv"
    df.to_csv(file_destination,  index=False)


def France():
    global location_list
    file_source = r"/root/airflow/dags/EU_Folder/EU_DataFrame.csv"
    location_list.append(file_source)
    df = pd.read_csv(file_source)
    df = df['France']
    file_destination = r"/root/airflow/dags/EU_Folder/France.csv"
    df.to_csv(file_destination,  index=False)


def Germany():
    global location_list
    file_source = r"/root/airflow/dags/EU_Folder/EU_DataFrame.csv"
    location_list.append(file_source)
    df = pd.read_csv(file_source)
    df = df['Germany']
    file_destination = r"/root/airflow/dags/EU_Folder/Germany.csv"
    df.to_csv(file_destination,  index=False)


def Greece():
    global location_list
    file_source = r"/root/airflow/dags/EU_Folder/EU_DataFrame.csv"
    location_list.append(file_source)
    df = pd.read_csv(file_source)
    df = df['Greece']
    file_destination = r"/root/airflow/dags/EU_Folder/Greece.csv"
    df.to_csv(file_destination,  index=False)


def Ireland():
    global location_list
    file_source = r"/root/airflow/dags/EU_Folder/EU_DataFrame.csv"
    location_list.append(file_source)
    df = pd.read_csv(file_source)
    df = df['Ireland']
    file_destination = r"/root/airflow/dags/EU_Folder/Ireland.csv"
    df.to_csv(file_destination,  index=False)


def Italy():
    global location_list
    file_source = r"/root/airflow/dags/EU_Folder/EU_DataFrame.csv"
    location_list.append(file_source)
    df = pd.read_csv(file_source)
    df = df['Italy']
    file_destination = r"/root/airflow/dags/EU_Folder/Italy.csv"
    df.to_csv(file_destination,  index=False)


def Luxembourg():
    global location_list
    file_source = r"/root/airflow/dags/EU_Folder/EU_DataFrame.csv"
    location_list.append(file_source)
    df = pd.read_csv(file_source)
    df = df['Luxembourg']
    file_destination = r"/root/airflow/dags/EU_Folder/Luxembourg.csv"
    df.to_csv(file_destination,  index=False)


def Netherlands():
    global location_list
    file_source = r"/root/airflow/dags/EU_Folder/EU_DataFrame.csv"
    location_list.append(file_source)
    df = pd.read_csv(file_source)
    df = df['Netherlands']
    file_destination = r"/root/airflow/dags/EU_Folder/Netherlands.csv"
    df.to_csv(file_destination,  index=False)


def Portugal():
    global location_list
    file_source = r"/root/airflow/dags/EU_Folder/EU_DataFrame.csv"
    location_list.append(file_source)
    df = pd.read_csv(file_source)
    df = df['Portugal']
    file_destination = r"/root/airflow/dags/EU_Folder/Portugal.csv"
    df.to_csv(file_destination,  index=False)


def Spain():
    file_source = r"/root/airflow/dags/EU_Folder/EU_DataFrame.csv"
    location_list.append(file_source)
    df = pd.read_csv(file_source)
    df = df['Spain']
    file_destination = r"/root/airflow/dags/EU_Folder/Spain.csv"
    df.to_csv(file_destination,  index=False)


def United_Kingdom():
    global location_list
    file_source = r"/root/airflow/dags/EU_Folder/EU_DataFrame.csv"
    location_list.append(file_source)
    df = pd.read_csv(file_source)
    df = df['United Kingdom']
    file_destination = r"/root/airflow/dags/EU_Folder/United Kingdom.csv"
    df.to_csv(file_destination,  index=False)
```
## Generate HeatMap 
```Python3
def heatMap():
    file_source = r"/root/airflow/dags/eu_terrorism_fatalities_by_country.csv"
    df = pd.read_csv(file_source)
    df = df.set_index('iyear')
    colormap = sns.diverging_palette(220, 10)
    plt.figure(figsize=(15, 10))
    sns.heatmap(df, annot=True, cmap=colormap, linecolor='black', linewidth=2, fmt='g')
    plt.suptitle('Terror Attacks in Europe (1970-2014)')
    plt.xlabel('Country')
    # Apply xticks
    plt.xticks(ha='center')
    # Apply yticks
    plt.yticks(range(len(df.index)), df.index)
    plt.tight_layout()
    plt.savefig(...home/example_location/output_folder)
```

