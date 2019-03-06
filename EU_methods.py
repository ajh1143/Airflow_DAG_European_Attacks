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


def load():
    file_source = r"/root/airflow/dags/eu_terrorism_fatalities_by_country.csv"
    df = pd.read_csv(file_source)
    file_destination = r"/root/airflow/dags/EU_Folder/EU_DataFrame.csv"
    df.to_csv(file_destination)


def count_records():
    file_source = r"/root/airflow/dags/EU_Folder/EU_DataFrame.csv"
    df = pd.read_csv(file_source)
    print("Collected {} records".format(len(df)))


def count_columns():
    file_source = r"/root/airflow/dags/EU_Folder/EU_DataFrame.csv"
    df = pd.read_csv(file_source)
    print("Contains: {} columns".format(len(df.columns)))


def list_countries():
    file_source = r"/root/airflow/dags/EU_Folder/EU_DataFrame.csv"
    df = pd.read_csv(file_source)
    print("Countries: {}".format(list(df.columns)))


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
    plt.savefig(r'C:\Users\\andrew.holt\AppData\Local\Packages\CanonicalGroupLimited.UbuntuonWindows_79rhkp1fndgsc\LocalState\\rootfs\\root\\airflow\dags\EU_Folder\\hm_output.png')


