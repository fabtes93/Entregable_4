from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
import psycopg2
import pandas as pd
import marvel
from airflow.operators.email_operator import EmailOperator
from airflow.utils.email import send_email


public_key = Variable.get("public_key")
private_key = Variable.get("private_key")


redshift_conn_id = 'redshift_default'


default_args = {
            'owner': 'FabiT',
            'email': 'SMTP_EMAIL_FROM',
            'start_date': datetime(2023, 7, 9),
            'retries':2,
            'retry_delay': timedelta(minutes=2),
            }

host = Variable.get("redshift_host")
port = Variable.get("redshift_port")
database = Variable.get("redshift_database")
user = Variable.get("redshift_user")
password = Variable.get("redshift_password")
table_name = Variable.get("redshift_table_name")

dag = DAG('marvel_dag', default_args=default_args, schedule_interval=None)

def buscarinfomarvel():
    marvel_api = marvel.Marvel(public_key, private_key)
    characters = marvel_api.characters.all()
    offset = 0
    limit = 99
    total_results = 0
    characters_data = []

    while total_results < limit:
        response = marvel_api.characters.all(limit=limit, offset=offset)
        data = response['data']
        results = data['results']
        total_results += len(results)
        characters_data.extend(results)
        offset += len(results)

    return characters_data

def personajesmarvel():
    characters_data = buscarinfomarvel()

    data = []
    for character in characters_data:
        name = character['name']
        comics = ', '.join([comic['name'] for comic in character['comics']['items']])
        series = ', '.join([serie['name'] for serie in character['series']['items']])
        description = character['description']
        alignment = character_info['alignment']
        data.append([name, comics, series, description, alignment])
        

    df = pd.DataFrame(data, columns=['name', 'comics', 'series', 'description', 'alignment'])
    df['Apariciones_personajes'] = df['comics'].apply(lambda x: len(x.split(', ')))

    return df

def guardar_csv(**context):
    ti = context['task_instance']
    df = ti.xcom_pull(task_ids='personajesmarvel')
    csv_filename = '/tmp/marvel_data.csv'
    df.to_csv(csv_filename, index=False)
    return csv_filename

def cargar_datos_redshift(**context):
    csv_filename = context['task_instance'].xcom_pull(task_ids='guardar_csv')
    
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    cursor = conn.cursor()

    try:
        cursor.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
        result = cursor.fetchone()
        if result:
            print("La tabla existe en Redshift.")
        else:
            print("La tabla no existe en Redshift.")
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    name VARCHAR(255),
                    comics VARCHAR(255),
                    series VARCHAR(255),
                    description TEXT,
                    alignment VARCHAR(50),
                    Apariciones_personajes INTEGER,
                    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                DISTKEY(name)
                SORTKEY(Apariciones_personajes)
            """)
            conn.commit()
            print("Tabla creada en Redshift.")

        # Cargar datos desde el CSV a la tabla en Redshift
        with open(csv_filename, 'r') as f:
            cursor.copy_from(f, table_name, sep=',', columns=('name', 'comics', 'series', 'description', 'Apariciones_personajes', 'fecha_carga'))

        conn.commit()
        print("Datos cargados en Redshift.")
    except Exception as e:
        print(f"Error al verificar la tabla: {str(e)}")
    finally:
        cursor.close()
        conn.close()


def enviar_error(context):
    execution_date = context['execution_date']
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    message = f"Error en el DAG '{dag_id}' Tarea '{task_id}' Tiempo: {execution_date}"
    enviar_alerta_email(message)

def enviar_success(context):
    execution_date = context['execution_date']
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    message = f"El DAG '{dag_id}' terminó correctamente Tarea '{task_id}' Tiempo: {execution_date}"
    enviar_alerta_email(message)

def enviar_alerta_email(message):
    email_subject = "Alerta desde airflow"
    email_to = [Variable.get('SMTP_EMAIL_TO')]
    email_from = Variable.get('SMTP_EMAIL_FROM')
    email_operator = EmailOperator(
        task_id='enviar_alerta_email',
        to=email_to,
        subject=email_subject,
        html_content=message,
        mime_charset='utf-8',
        params=None,
        cc=None,
        bcc=None,
        reply_to=None,
        retries=3,
        retry_delay=timedelta(minutes=1)
    )
    email_operator.execute(context=None)


with dag:
    task_buscarinfomarvel = PythonOperator(
        task_id='buscarinfomarvel',
        python_callable=buscarinfomarvel,
        depends_on_past=True
    )

    task_personajesmarvel = PythonOperator(
        task_id='personajesmarvel',
        python_callable=personajesmarvel,
        depends_on_past=True
    )

    task_guardar_csv = PythonOperator(
        task_id='guardar_csv',
        python_callable=guardar_csv,
        depends_on_past=True
    )

    task_tablamarvel = PythonOperator(
        task_id='tablamarvel',
        python_callable=cargar_datos_redshift,
        depends_on_past=True 
    )

    task_error = PythonOperator(
        task_id='dag_envio_error',
        python_callable=enviar_error,
        trigger_rule='all_failed',
        provide_context=True
    )
    
    task_ok = PythonOperator(
        task_id='dag_envio_success',
        python_callable=enviar_success,
        trigger_rule='all_success',
        provide_context=True
    )

    task_guardar_csv.set_upstream(task_personajesmarvel)
    task_tablamarvel.set_upstream(task_guardar_csv)
    task_tablamarvel.set_downstream([task_error, task_ok])
    
    task_buscarinfomarvel >> task_personajesmarvel >> task_guardar_csv >> task_tablamarvel >> [task_error, task_ok]



    
    

    
    
