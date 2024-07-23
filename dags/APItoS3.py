from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Python Function to call Public API 
def api_call(**kwargs):
    
    ti = kwargs['ti']
    url = 'https://api.publicapis.org/entries'
    response = requests.get(url).json()
    df = pd.DataFrame(response['entries'])
    ti.xcom_push(key = 'final_data' , value = df.to_csv(index=False))

# We are using xcom to transfer the data from python task to S3 task 

with dag:


    api_hit_task = PythonOperator(
        task_id = 'API-Call',
        python_callable = api_call,
        provide_context = True,
        
    )


from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

with dag:
 
    Upload_to_s3 = S3CreateObjectOperator(
        task_id="Upload-to-S3",
        aws_conn_id= 'AWS_CONN',
        s3_bucket='pch-test-bucket',
        s3_key='rajesh/test_Data.csv',
        data="{{ ti.xcom_pull(key='final_data') }}",    
    )

    api_hit_task >> Upload_to_s3

