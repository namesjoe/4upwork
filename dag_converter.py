from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from Logger import logger
import sqlite3
from converter import Converter
import concurrent
import numpy as np


class SQLite():
    def __init__(self, file="/home/sedov/repos/4upwork/mydb.db"):
        self.file = file

    def __enter__(self):
        self.conn = sqlite3.connect(self.file)
        return self.conn.cursor()

    def __exit__(self, type, value, traceback):
        self.conn.commit()
        self.conn.close()


default_args = {
    'owner': 'sedov',
    'depends_on_past': False,
    'email': ['namesoe@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


def start_it():
    start_sql = """
    UPDATE files
    SET status = 'S'
    WHERE
        status = 'P';
    """

    sql = "SELECT * FROM files WHERE status = 'S';"
    with SQLite() as cursor:
        cursor.execute(start_sql)
        cursor.execute(sql)
        df = np.array(cursor.fetchall())
    logger.info(f'Found {len(df)} files to convert')
    conv = Converter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        results = executor.map(conv.convert, df[:, 0], df[:, 1], df[:, 2], df[:, 3])
        for result in results:
            pass


def on_failure_callback(context):
    fail_sql = """
    UPDATE files
    SET status = 'F'
    WHERE
        status = 'S' 
    """
    with SQLite() as curs:
        curs.execute(fail_sql)
    logger.info('Rolling back')


def update_statuses():
    update_status_sql = """
    UPDATE files
    SET status = 'C'
    WHERE
        status = 'P'
    """
    with SQLite() as curs:
        curs.execute(update_status_sql)
    logger.info('Updated statuses to success ')


with DAG('files_converter_dag', default_args=default_args,
         description='upwork dag', schedule_interval=timedelta(days=1),
         start_date=days_ago(7), max_active_runs=1) as dag:

    t1 = PythonOperator(task_id='start_it', python_callable=start_it, on_failure_callback=on_failure_callback)
    t2 = PythonOperator(task_id='update_statuses', python_callable=update_statuses)


t1 >> t2

