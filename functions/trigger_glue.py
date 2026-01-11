# functions/trigger_glue.py
import boto3
import json
import os
import urllib.parse

glue = boto3.client('glue')

def lambda_handler(event, context):
    glue_job_name = os.environ.get('GLUE_JOB_NAME', 'transform_job')

    bucket = None
    key = None

    # 1) Evento padrão do S3 (Bucket Notification -> Lambda)
    if isinstance(event, dict) and 'Records' in event and event['Records']:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    # 2) Invocação manual / teste (permite passar {"bucket": "...", "key": "..."})
    elif isinstance(event, dict) and 'bucket' in event and 'key' in event:
        bucket = event['bucket']
        key = event['key']

    if not bucket or not key:
        print('Evento inesperado (sem Records e sem bucket/key).')
        print(json.dumps(event))
        return {'statusCode': 400, 'body': 'Evento inválido: esperado S3 Records ou bucket/key.'}
    
    print(f"Arquivo detectado: s3://{bucket}/{key}")
    
    try:
        # Passa o nome do bucket e o caminho do arquivo como argumentos para o Glue
        arguments = {
            '--BUCKET_NAME': bucket,
            '--INPUT_KEY': key,
            '--additional-python-modules': 'polars,yfinance'
        }
        
        response = glue.start_job_run(JobName=glue_job_name, Arguments=arguments)
        print(f"Glue Job iniciado: {response['JobRunId']}")
        return {'statusCode': 200, 'body': 'Job iniciado.'}
        
    except Exception as e:
        print(e)
        raise e