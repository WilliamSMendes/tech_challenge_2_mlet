# functions/trigger_glue.py
import boto3
import json
import os
import urllib.parse
from botocore.exceptions import ClientError

glue = boto3.client('glue')


def _has_active_run(glue_job_name: str) -> bool:
    try:
        response = glue.get_job_runs(JobName=glue_job_name, MaxResults=10)
        for job_run in response.get('JobRuns', []):
            if job_run.get('JobRunState') in {'STARTING', 'RUNNING', 'STOPPING'}:
                return True
        return False
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code')
        if code in {'AccessDeniedException', 'AccessDenied'}:
            print(f"Sem permissão para glue:GetJobRuns em '{glue_job_name}'. Continuando sem checagem prévia.")
            return False
        raise

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
        if _has_active_run(glue_job_name):
            print(f"Glue Job '{glue_job_name}' já está em execução. Ignorando novo disparo.")
            return {'statusCode': 202, 'body': 'Job já em execução; trigger ignorado.'}

        # Passa o nome do bucket e o caminho do arquivo como argumentos para o Glue
        arguments = {
            '--BUCKET_NAME': bucket,
            '--INPUT_KEY': key,
            '--additional-python-modules': 'polars,yfinance'
        }
        
        response = glue.start_job_run(JobName=glue_job_name, Arguments=arguments)
        print(f"Glue Job iniciado: {response['JobRunId']}")
        return {'statusCode': 200, 'body': 'Job iniciado.'}
        
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code')
        if code == 'ConcurrentRunsExceededException':
            print(f"Concurrent runs exceeded para '{glue_job_name}'. Considerando OK (já existe execução em andamento).")
            return {'statusCode': 202, 'body': 'Job já em execução; concorrência excedida.'}

        print(e)
        raise
    except Exception as e:
        print(e)
        raise