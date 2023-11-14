from bs4 import BeautifulSoup
import requests as req
import pandas as pd
import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}

def get_ids(res):
    soup = BeautifulSoup(res.text, 'html.parser')
    tbodys = soup.findall('tbody')
    trs = tbodys[11].findall('tr')
    ths = []
    codigos = []
    equipos = []

    for tr in trs:
        ths.append(tr.find('th', class_='left'))
    for th in ths:
        texto_split = th.split('href="/es/equipos/')
        codigos.append(texto_split[1].split('/Estadisticas')[0])
        equipos.append(texto_split[1].split('>vs')[1].split('</a>')[0].strip())

    return equipos, codigos

def scraping():
    url = 'http://fbref.com/es/comps/9/Estadisticas-de-Premier-League'
    res = req.get(url, headers = headers)
    equipos, codigos = get_ids(res)

    df = pd.DataFrame({'Codigo': codigos, 'Equipo': equipos})
    df.to_csv('/opt/airflow/dags/Equipos.csv', index=False)

    return 'Equipos.csv'

def add_team(**kwargs):
    ti = kwargs['ti']
    file_name = ti.xcom_pull(task_ids = 'task_scraping')

    destination_file = f'/opt/airflow/dags/{file_name}'
    df = pd.read_csv(destination_file)
    new_team = {'Codigo': '643108', 'Equipo': 'The_sups_team'}
    df = df.append(new_team, ignore_index=True)
    df.to_csv(destination_file, index=False)
    

default_args = {
    'owner': 'Juan',
    'depends_on_past': True,
    'start_date': datetime.datetime(2023, 11, 9),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=.5)
}

with DAG('Scraping',
         default_args = default_args,
         schedule_interval = '0 0 * * 0') as dag:
    
    t_begin = DummyOperator(task_id='begin')

    task_scraping = PythonOperator(task_id='task_scraping',
                                   python_callable = scraping,
                                   depends_on_past = True,
                                   
                                   dag=dag)
    task_add_team = PythonOperator(task_id='task_add_team',
                                   provide_context = True,
                                   python_callable = add_team,
                                   depends_on_past = True,

                                   dag=dag)
    
    t_end = DummyOperator(task_id = 'end')

t_begin >> task_scraping >> task_add_team >> t_end