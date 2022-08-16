from datetime import datetime,timedelta
import pandas as pd
from airflow import DAG
from breakout_indicator.util_functions import *
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.operators.email import EmailOperator
from airflow.models import taskinstance

default_args ={
            "owner":"Dolly",
            "retries":1,
            "retry_delay":timedelta(minutes=1),
            "email":['XXXXXXXXXXXXXX@gmail.com'],
            "email_on_failure":False,
            "email_on_retry":False,
}

with DAG ( dag_id = "price_action_analysis",
            default_args = default_args,
            start_date = datetime(2022,8,9),
            schedule_interval ='@daily',
            catchup = False
        ) as dag:

        get_symb_list = PythonOperator(
                            task_id = "get_symb_list",
                            python_callable=get_sym_list
                            )

        update_data_sql = PythonOperator(
                        task_id="update_data_sql",
                        python_callable=update_data
                        ) 

        calc_resistance_supp = PythonOperator(
                        task_id="calc_resistance_support",
                        python_callable=get_resistance_support
                        )          

        send_email = EmailOperator(
                        task_id="send_email",
                        to='XXXXXXXXXXX@gmail.com',
                        subject="AIRFLOW: Dag Run: {{ dag_run }}, Exec Date: {{ execution_date }}",
                        html_content="""
                                        <h2> Find below stocks that are closer to their Resistance or Support: </h2><br>
                                        <h3> Resistance list: </h3> <p>{{ti.xcom_pull(task_ids='calc_resistance_support',key='resistance_list')}}</p><br>
                                        <h3> Support List list: </h3> <p>{{ti.xcom_pull(task_ids='calc_resistance_support',key='support_list')}}</p>
                                    """,                        
                        )

        get_symb_list >> update_data_sql >> calc_resistance_supp  >> send_email