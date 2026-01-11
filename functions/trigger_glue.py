import boto3
import json

glue = boto3.client('glue')

def lambda_handler(event, context):
    print("Evento recebido:", json.dumps(event))
    glue_job_name = 'transform_job' 
    
    # Verifica se o evento veio do EventBridge (Scheduled)
    if 'source' in event and event['source'] == 'aws.events':
        print("Acionado via Agendamento (EventBridge).")
    else:
        # Lógica antiga para S3 (caso mantenha o upload manual como opção)
        print("Acionado via Upload S3 ou outro método.")

    try:
        # Inicia o Job sem argumentos específicos, o Job calcula D-1 internamente
        response = glue.start_job_run(JobName=glue_job_name)
        print(f"Glue Job {glue_job_name} iniciado. JobRunId: {response['JobRunId']}")
        return {'statusCode': 200, 'body': 'Job disparado.'}
        
    except Exception as e:
        print(f"Erro: {str(e)}")
        raise e