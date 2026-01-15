# functions/trigger_glue.py
"""
trigger_glue.py - Lambda para acionar job Glue
Recebe eventos S3 e inicia o job de transformacao no AWS Glue.
"""
import boto3
import json
import os
import urllib.parse
from botocore.exceptions import ClientError

glue = boto3.client('glue')


def _has_active_run(glue_job_name: str) -> bool:
    """
    Verifica se existe uma execucao ativa do job Glue.
    
    Args:
        glue_job_name: Nome do job Glue
    
    Returns:
        True se houver execucao ativa, False caso contrario
    """
    try:
        response = glue.get_job_runs(JobName=glue_job_name, MaxResults=10)
        for job_run in response.get('JobRuns', []):
            if job_run.get('JobRunState') in {'STARTING', 'RUNNING', 'STOPPING'}:
                return True
        return False
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code')
        if code in {'AccessDeniedException', 'AccessDenied'}:
            print(f"Sem permissao para glue:GetJobRuns em '{glue_job_name}'. Continuando sem checagem previa.")
            return False
        raise


def lambda_handler(event, context):
    """
    Handler principal da Lambda Function.
    Processa evento S3 e inicia job Glue para transformacao de dados.
    
    Args:
        event: Evento S3 ou invocacao manual com bucket/key/prefix
        context: Contexto da Lambda
    
    Returns:
        Dict com statusCode e body contendo resultado da execucao
    """
    glue_job_name = os.environ.get('GLUE_JOB_NAME', 'transform_job')

    bucket = None
    key = None
    prefix = None

    if isinstance(event, dict) and 'Records' in event and event['Records']:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    elif isinstance(event, dict) and 'bucket' in event and ('key' in event or 'prefix' in event):
        bucket = event['bucket']
        key = event.get('key')
        prefix = event.get('prefix')

    if not bucket or (not key and not prefix):
        print('Evento inesperado (sem Records e sem bucket/key/prefix).')
        print(json.dumps(event))
        return {'statusCode': 400, 'body': 'Evento invalido: esperado S3 Records ou bucket/key/prefix.'}

    if not prefix:
        if not key:
            return {'statusCode': 400, 'body': 'Evento invalido: key ausente.'}
        if key.endswith('_SUCCESS'):
            prefix = key.rsplit('/', 1)[0] + '/'
        else:
            prefix = key.rsplit('/', 1)[0] + '/'

    print(f"Prefixo detectado: s3://{bucket}/{prefix}")
    
    try:
        if _has_active_run(glue_job_name):
            print(f"Glue Job '{glue_job_name}' ja esta em execucao. Ignorando novo disparo.")
            return {'statusCode': 202, 'body': 'Job ja em execucao; trigger ignorado.'}

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
            print(f"Concurrent runs exceeded para '{glue_job_name}'. Considerando OK (ja existe execucao em andamento).")
            return {'statusCode': 202, 'body': 'Job ja em execucao; concorrencia excedida.'}

        print(e)
        raise
    except Exception as e:
        print(e)
        raise