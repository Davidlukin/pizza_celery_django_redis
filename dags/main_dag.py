from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def search():
    try:
        conn = psycopg2.connect(dbname='airflow', host='newpgadmin', user='airflow', password='airflow', port='5432')
        cursor = conn.cursor()
        cursor.execute('SELECT email, admin FROM test')
        rows = cursor.fetchall()
        data = [(row[0], row[1]) for row in rows]
        print(data)
        return data
    except psycopg2.Error as e:
        print(f"Error: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def send_email(recipients):
    smtp_server = smtplib.SMTP("smtp.mail.ru", 587)
    smtp_server.starttls()
    smtp_server.login("david.lukin.99@bk.ru", "DwPsxVqbMLRBN62crSfz")
    try:
        for recipient, admin in recipients:
            if not admin:
                msg = MIMEMultipart()

                msg["From"] = "david.lukin.99@bk.ru"
                msg["To"] = recipient
                msg["Subject"] = "Пицца у давида"

                # Добавление текста в сообщение
                text = "Ваш заказ обрабатывает администратор"
                msg.attach(MIMEText(text, "plain"))

                # Отправка письма
                smtp_server.sendmail("david.lukin.99@bk.ru", recipient, msg.as_string())
    except smtplib.SMTPException as e:
        print(f"SMTP Error: {e}")
    finally:
        smtp_server.quit()


def sendleteer_now():
    recipients = search()
    send_email(recipients)


def search_and_update():
    try:
        conn = psycopg2.connect(dbname='airflow', host='newpgadmin', user='airflow', password='airflow', port='5432')
        cursor = conn.cursor()

        cursor.execute('SELECT email, admin FROM test WHERE admin = false')
        rows = cursor.fetchall()

        for row in rows:
            email = row[0]
            cursor.execute('UPDATE test SET admin = true WHERE email = %s', (email,))

        conn.commit()

    except psycopg2.Error as e:
        print(f"Error: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

#отправка промокода

def send_email_promocode(recipients, **kwargs):
    smtp_server = smtplib.SMTP("smtp.mail.ru", 587)
    smtp_server.starttls()
    smtp_server.login("david.lukin.99@bk.ru", "DwPsxVqbMLRBN62crSfz")
    try:
        for recipient, admin in recipients:
            if  admin == True:

                msg = MIMEMultipart()

                msg["From"] = "david.lukin.99@bk.ru"
                msg["To"] = recipient
                msg["Subject"] = "Пицца у давида"

                text = "Ваш промокод ПРИВЕТ дает 20% скидки на все пиццы"
                msg.attach(MIMEText(text, "plain"))

                # Отправка письма
                smtp_server.sendmail("david.lukin.99@bk.ru", recipient, msg.as_string())

                ti = kwargs['ti']
                ti.xcom_push(key='email_sent', value=True)

    except smtplib.SMTPException as e:
        print(f"SMTP Error: {e}")
    finally:
        smtp_server.quit()


def print_message_mistake(**kwargs):
    ti = kwargs['ti']
    email_sent = ti.xcom_pull(key='email_sent', task_ids='send_email_task', default=False)

    if email_sent:
        print("Данные успешно отправились")
    else:
        print("Данные не отаравились")


def send_email_ril_promocode():
    data = search()
    send_email_promocode(data)


def send_email_evryone_good():
    smtp_server = smtplib.SMTP("smtp.mail.ru", 587)
    smtp_server.starttls()
    smtp_server.login("david.lukin.99@bk.ru", "DwPsxVqbMLRBN62crSfz")
    try:

                msg = MIMEMultipart()

                msg["From"] = "david.lukin.99@bk.ru"
                msg["To"] = 'vasgsa@bk.ru'
                msg["Subject"] = "Ошибки небыло"

                text = "Даг работает коректно"
                msg.attach(MIMEText(text, "plain"))

                smtp_server.sendmail("david.lukin.99@bk.ru", 'vasgsa@bk.ru', msg.as_string())

    except smtplib.SMTPException as e:
        print(f"SMTP Error: {e}")
    finally:
        smtp_server.quit()



default_args = {
    'owner': 'david',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('send_email_latter', description='send_letter', schedule_interval='@hourly', catchup=False, default_args=default_args) as dag:

    t1 = PythonOperator(
        task_id='connect_bd',
        python_callable=search,
        provide_context=True
    )

    t2 = PythonOperator(
        task_id='send_letter',
        python_callable= sendleteer_now,
        provide_context=True
    )


    t3 = PythonOperator(
        task_id='send_letter_true',
        python_callable=search_and_update,
        provide_context=True
    )


    t4 = PythonOperator(
        task_id='send_promocde_data',
        python_callable=send_email_ril_promocode,
        provide_context=True
    )

    t5 = PythonOperator(
        task_id='mistake',
        python_callable=print_message_mistake,
        provide_context=True
    )

    t6 = PythonOperator(
        task_id='good',
        python_callable=send_email_evryone_good,
        provide_context=True
    )


    t1 >> t2>>t3>>t4
    t4<<t5
    t4<<t6