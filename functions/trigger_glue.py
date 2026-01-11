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
    prefix = None

    # 1) Evento padrão do S3 (Bucket Notification -> Lambda)
    if isinstance(event, dict) and 'Records' in event and event['Records']:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    # 2) Invocação manual / teste
    # - {"bucket": "...", "key": "..."}
    # - {"bucket": "...", "prefix": "raw/.../"}
    elif isinstance(event, dict) and 'bucket' in event and ('key' in event or 'prefix' in event):
        bucket = event['bucket']
        key = event.get('key')
        prefix = event.get('prefix')

    if not bucket or (not key and not prefix):
        print('Evento inesperado (sem Records e sem bucket/key/prefix).')
        print(json.dumps(event))
        return {'statusCode': 400, 'body': 'Evento inválido: esperado S3 Records ou bucket/key/prefix.'}

    if not prefix:
        if not key:
            return {'statusCode': 400, 'body': 'Evento inválido: key ausente.'}
        # Se for marker de finalização, processa o prefixo inteiro da execução
        if key.endswith('_SUCCESS'):
            prefix = key.rsplit('/', 1)[0] + '/'
        else:
            # Para compatibilidade: se vier um parquet específico, processa o diretório dele
            prefix = key.rsplit('/', 1)[0] + '/'

    print(f"Prefixo detectado: s3://{bucket}/{prefix}")
    
    try:
        if _has_active_run(glue_job_name):
            print(f"Glue Job '{glue_job_name}' já está em execução. Ignorando novo disparo.")
            return {'statusCode': 202, 'body': 'Job já em execução; trigger ignorado.'}

        # Passa o nome do bucket e o prefixo de entrada (dataset particionado) para o Glue
        arguments = {
            '--BUCKET_NAME': bucket,
            '--INPUT_PREFIX': prefix,
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