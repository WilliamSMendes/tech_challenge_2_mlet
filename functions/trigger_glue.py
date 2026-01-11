# functions/trigger_glue.py
import boto3
import urllib.parse

glue = boto3.client('glue')

def lambda_handler(event, context):
    glue_job_name = 'transform_job'
    
    # Pega o bucket e a key do evento S3
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
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