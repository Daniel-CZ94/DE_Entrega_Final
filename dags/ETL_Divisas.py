import json
import pandas as pnd
import requests
from datetime import date,timedelta,datetime
import psycopg2
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowSkipException
import os
import smtplib
from email import message
from cryptography.fernet import Fernet

BASE_URL_API = None #URL de la API de donde obtendremos los tipos de cambio
BASE_CURRENCY = None #Divisa base de la cual se obtendra la conversion a otras divisas

BASE_BD_HOST = None #Servidor de base de datos
BASE_BD_NAME = None #Nombre de la base de datos
BASE_BD_USER = None #Usuario de la base de datos
BASE_BD_PASS = None #Contraseña de la base de datos
BASE_BD_PORT = None #Puerto de la base de datos

BASE_EMAIL_USER = None #Email de salida de las notificaciones
BASE_EMAIL_PASS_VALUE = None #Contraseña del mail de notificaciones
BASE_EMAIL_PASS_KEY = None #Key para el logeo del mail de notificaciones
BASE_EMAIL_TO = None #Email de destino para las notificaciones

dag_path = os.getcwd()

with open(dag_path+'/keys/config.json','r') as file:
    config = json.load(file)

BASE_URL_API = config['DATA_API']['URL_BASE']
BASE_CURRENCY = config['DATA_API']['FROM_CURRENCY']
BASE_CURRENCY_PREFERRED = config['DATA_API']['CURRENCY_PREFERRED']
BASE_CURRENCY_RATE_MINIMUM = config['DATA_API']['MINIMUM_RATE_CURRENCY']
        
BASE_BD_HOST = config['DATA_BD']['BD_HOST']
BASE_BD_NAME = config['DATA_BD']['BD_NAME']
BASE_BD_USER = config['DATA_BD']['BD_USER']
BASE_BD_PASS = config['DATA_BD']['BD_PASS']
BASE_BD_PORT = config['DATA_BD']['BD_PORT']

TABLE_NAME = config['DATA_BD']['DATA_TABLE']['NAME_TABLE']
TABLE_COLUMNS = config['DATA_BD']['DATA_TABLE']['COLUMNS']

BASE_EMAIL_USER = config['EMAIL_DATA']['EMAIL_FROM_USER']
BASE_EMAIL_TO = config['EMAIL_DATA']['EMAIL_TO']

CARGA_ACTUAL_REALIZADA = None

default_args = {
    'owner':'DanielCZ',
    'start_date': datetime(2023,6,16),
    'retries':0,
    'retry_delay':timedelta(minutes=1)
}
BC_dag = DAG(
    dag_id='Divisas_ETL',
    default_args=default_args,
    description='Obtiene el tipo de cambio de una divisa especifica a varias divisas de Lunes a Viernes',
    schedule_interval='@daily',
    catchup=False
)

dag_path = os.getcwd()

#Los tipos de cambios solo se actualizan de Lunes a Viernes
#Esta funcionalidad valida que el dia actual de carga sea entre Lunes y Viernes
#En caso de que sea fin de semana, el proceso se detendra
def validar_dia_habil(date_execution,**kwargs):
    try:
        print("Validamos que el dia actual sea un dia habil")
        print(date_execution)
        fecha = datetime.strptime(date_execution, '%Y-%m-%d')
        dia_semana = fecha.weekday()
        dia_habil = None
        if dia_semana < 5:
            dia_habil = True
        else:
            dia_habil = False
            enviar_correo("Proceso cancelado","El dia actual es un dia inhabil. Por lo tanto no se realizara la carga")
            raise AirflowSkipException
        kwargs['ti'].xcom_push(key='dia_habil',value=dia_habil)
    except Exception as error:
        print("Surgio un error al validar el dia habil: "+error)
        enviar_correo("Error en validar_dia_habil","Surgio un error al validar el dia habil: "+error)

#Consultamos la API de tipos de cambio y obtenemos los registros correspondientes
def obtener_tipo_cambio_actual(date_execution,**kwargs):
    try:
        print("Obteniendo el tipo de cambio actual")
        ti = kwargs['ti']
        es_dia_habil = None
        es_dia_habil = ti.xcom_pull(key='dia_habil')
        print('xcom_pull=dia_habil')
        print(es_dia_habil)
        if(es_dia_habil):
            url_request = f"{BASE_URL_API}/{date_execution}?from={BASE_CURRENCY}"
            request_currencies = None
            data_currencies = None
            request_currencies = requests.get(url_request)
            if request_currencies.status_code == 200:
                data_currencies = request_currencies.json()
                with open(dag_path+"/raw_data/"+"data_"+str(date_execution)+".json","w") as json_file:
                    json.dump(data_currencies,json_file)
            else:
                print("La peticion al servidor de los tipos de cambio ha fallado")
        else:
            print("El dia actual es un dia inhabil. Por lo tanto no se realizara la carga")
    except Exception as ex:
        print("Surgio un error al obtener la informacion desde el servidor de tipos de cambio: "+ex)
        enviar_correo("Error en obtener_tipo_cambio_actual","Surgio un error al obtener la informacion desde el servidor de tipos de cambio: "+ex)

#Ejecutamos la creacion de la tabla de divisas en caso de que esta no exista en la BD
def crear_tabla_carga_divisas(**kwargs):
    try:
        print("Creamos la tabla de destino")
        ti = kwargs['ti']
        es_dia_habil = None
        es_dia_habil = ti.xcom_pull(key='dia_habil')
        if(es_dia_habil):
            conn = psycopg2.connect(
                host = BASE_BD_HOST,
                dbname = BASE_BD_NAME,
                user = BASE_BD_USER,
                password = BASE_BD_PASS,
                port = BASE_BD_PORT
            )
            sql_table = f"""
            create table if not exists {TABLE_NAME}(
                currency varchar(3),
                operation_date date,
                ammount float,
                base_currency varchar(3),
                rates float,
                primary key(currency,operation_date),
                unique(currency,operation_date)
            )
            """
            cur = conn.cursor()
            cur.execute(sql_table)
            conn.commit()
        else:
            print("El dia actual es un dia inhabil. Por lo tanto no se realizara la carga")
    except Exception as error:
        print("Surgio un error al crear la tabla de carga: "+error)
        enviar_correo("Error en crear_tabla_carga_divisas","Surgio un error al crear la tabla de carga: "+error)

#Este metodo obtiene una el tipo de cambio de la divisa especificada en la configuracion
#asi como un valor minimo de este mismo tipo de cambio
#En caso de que el valor de la divisa establecida sea mayor al minimo establecido, se realizara el envio de 
# una notificacion por correo indicando lo anterior mencionado
def validar_tipo_cambio_minimo(date_execution,**kwargs):
    try:
        ti = kwargs['ti']
        es_dia_habil = None
        es_dia_habil = ti.xcom_pull(key='dia_habil')
        if(es_dia_habil):
            print(str(date_execution))
            registros_divisas = pnd.read_json(dag_path+"/raw_data/"+"data_"+str(date_execution)+".json")
            frame_currencies = pnd.DataFrame(registros_divisas)

            cambio_mx = frame_currencies.loc[BASE_CURRENCY_PREFERRED]['rates']
            if cambio_mx > float(BASE_CURRENCY_RATE_MINIMUM):
                enviar_correo('Cambio de {} a {}'.format(BASE_CURRENCY,BASE_CURRENCY_PREFERRED),'El valor de {} actual a {} ha rebasado del minimo deseado ({})'.format(BASE_CURRENCY,BASE_CURRENCY_PREFERRED,BASE_CURRENCY_RATE_MINIMUM))
        else:
            print("El dia actual es un dia inhabil. Por lo tanto no se realizara la carga")
        
    except Exception as ex:
        print("Surgio un error al validar el tipo de cambio minimo: "+ex)
        enviar_correo("Error en validar_tipo_cambio_minimo","Surgio un error al validar el tipo de cambio minimo: "+ex)

#Realizamos la carga de los datos en la BD
def cargar_divisas(date_execution,**kwargs):
    try:
        print("Cargando informacion en la BD")
        ti = kwargs['ti']
        es_dia_habil = None
        es_dia_habil = ti.xcom_pull(key='dia_habil')
        if(es_dia_habil):
            registros_divisas = pnd.read_json(dag_path+"/raw_data/"+"data_"+str(date_execution)+".json")
            frame_currencies = pnd.DataFrame(registros_divisas)
            frame_currencies.insert(0,'currency',frame_currencies.index)
            conn = psycopg2.connect(
                host = BASE_BD_HOST,
                dbname = BASE_BD_NAME,
                user = BASE_BD_USER,
                password = BASE_BD_PASS,
                port = BASE_BD_PORT
            )
            cols = ','.join(TABLE_COLUMNS)

            insert_sql = f"insert into {TABLE_NAME} ({cols}) values %s"
            values = [tuple(x) for x in registros_divisas.to_numpy()]
            cur = conn.cursor()
            cur.execute("BEGIN")
            execute_values(cur,insert_sql,values)
            conn.commit()
            print("Se realizo la carga de informacion de forma correcta")
            os.remove(dag_path+"/raw_data/"+"data_"+str(date_execution)+".json")
        else:
            print("El dia actual es un dia inhabil. Por lo tanto no se realizara la carga")
    except Exception as error:
        print("Surgio un error al cargar la informacion en la BD: "+error)
        enviar_correo("Error en cargar_divisas","Surgio un error al cargar la informacion en la BD: "+error)

#Metodo para el envio de correos
def enviar_correo(title,body):
    try:
        key = None
        email_pass = None
        with open(dag_path+"/keys/key_email.txt","rb") as f1, open(dag_path+"/keys/pass_email.txt","rb") as f2:
            key = f1.read()
            email_pass = f2.read()
        print(body)
        f = Fernet(key)
        decrypt_pass = str(f.decrypt(email_pass),'utf-8')
        x = smtplib.SMTP("smtp.gmail.com",587)
        x.starttls()
        x.login(BASE_EMAIL_USER,decrypt_pass)
        subject = title
        message = "Subject: {}\n\n{}".format(subject,body)
        x.sendmail(BASE_EMAIL_USER,BASE_EMAIL_TO,message)
        print("Correo enviado")
    except Exception as ex:
        print("Surgio un error al realizar el envio del correo: "+ex)

validar_dia_habil = PythonOperator(
    task_id='validar_dia_habil',
    python_callable=validar_dia_habil,
    op_args=["{{ ds }}"],
    op_kwargs={'my_task_id':'valida_1'},
    provide_context = True,
    dag=BC_dag
)
obtener_tipo_cambio_actual = PythonOperator(
    task_id='obtener_tipoCambioActual',
    python_callable=obtener_tipo_cambio_actual,
    op_args=["{{ ds }}"],
    dag=BC_dag
)
crear_tabla_carga_divisas = PythonOperator(
    task_id='crear_tabla_carga_divisas',
    python_callable=crear_tabla_carga_divisas,
    dag=BC_dag
)
validar_tipo_cambio_minimo = PythonOperator(
    task_id='validar_tipo_cambio_minimo',
    python_callable=validar_tipo_cambio_minimo,
    op_args=["{{ ds }}"],
    dag=BC_dag
)
cargar_divisas = PythonOperator(
    task_id='cargar_tipos_cambio',
    python_callable=cargar_divisas,
    op_args=["{{ ds }}"],
    dag=BC_dag
)

validar_dia_habil >> obtener_tipo_cambio_actual >> crear_tabla_carga_divisas >> validar_tipo_cambio_minimo  >> cargar_divisas