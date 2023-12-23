from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from sqlalchemy import create_engine
import json


import pandas

def search_exel():
    try:
        conn = psycopg2.connect(dbname='airflow', host='newpgadmin', user='airflow', password='airflow', port='5432')

        # SQLAlchemy
        engine = create_engine('postgresql://airflow:airflow@newpgadmin:5432/airflow')

        # Забираею данные через pandas
        df = pandas.read_sql('select * from test', con=engine)
        print(df)

        excel_filename = 'exported.xlsx'
        print(f"Excel file will be saved at: {excel_filename}")
        df.to_excel(excel_filename, index=False)

    except psycopg2.Error as e:
        print(f"Error: {e}")
        return []
    finally:
        if conn:
            conn.close()


def fetch_data_from_db():
    try:
        conn = psycopg2.connect(dbname='airflow', host='newpgadmin', user='airflow', password='airflow', port='5432')
        cursor = conn.cursor()

        cursor.execute('SELECT email, admin FROM test')
        rows = cursor.fetchall()
        data = [{'email': row[0], 'admin': row[1]} for row in rows]

        return data

    except psycopg2.Error as e:
        print(f"Error: {e}")
        return []

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def transform_and_print_data(**kwargs):
    ti = kwargs['ti']
    data_from_db = ti.xcom_pull(task_ids='fetch_data_task')

    # преобразование данных JSON
    json_data = json.dumps(data_from_db, indent=2)
    print(f'Transformed Data:\n{json_data}')




default_args = {
    'owner': 'david',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('send_exel_info', description='send_exel', schedule_interval='*/1 * * * *', catchup=False, default_args=default_args) as dag:

    t1 = PythonOperator(
        task_id='send',
        python_callable=search_exel,
        provide_context=True
    )

    t2 = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_data_from_db,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id='transform_and_print_task',
        python_callable=transform_and_print_data,
        provide_context=True,
    )


    t1>>t2>>t3