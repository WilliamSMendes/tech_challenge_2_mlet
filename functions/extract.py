# functions/extract.py
import json
import boto3
import os
from pathlib import Path

# Em AWS Lambda, não é garantido existir um HOME padrão com ~/.cache.
# O yfinance usa cache em ~/.cache/py-yfinance; então apontamos para /tmp e criamos o diretório.
os.environ.setdefault("HOME", "/tmp")
os.environ.setdefault("XDG_CACHE_HOME", os.path.join(os.environ["HOME"], ".cache"))
Path(os.path.join(os.environ["XDG_CACHE_HOME"], "py-yfinance")).mkdir(parents=True, exist_ok=True)

import yfinance as yf
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import tempfile

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    print("Iniciando Extração...")
    
    # Configuração de datas (D-1 e 6 meses atrás)
    today = datetime.now()
    end_date_obj = today - timedelta(days=1)
    end_date = end_date_obj.strftime('%Y-%m-%d')
    start_date_obj = end_date_obj - relativedelta(months=6)
    start_date = start_date_obj.strftime('%Y-%m-%d')
    
    tickers = ['TOTS3.SA', 'LWSA3.SA', 'POSI3.SA', 'INTB3.SA', 'WEGE3.SA']
    bucket_name = os.environ['BUCKET_NAME']
    
    # Cria diretório temporário para salvar o arquivo antes do upload
    with tempfile.TemporaryDirectory() as tmp_dir:
        file_path = f"{tmp_dir}/stocks_raw.parquet"
        
        try:
            df = yf.download(tickers=tickers, start=start_date, end=end_date, group_by='ticker', progress=False)
            
            # Usa stack e reseta o multiindex para ter virar dados tabulares
            df_stack = df.stack(level=0).reset_index().rename(columns={'level_1': 'Ticker'})
            
            # Salva localmente na lambda
            df_stack.to_parquet(file_path)
            
            # Define o caminho no S3 (Partição por dia de ingestão)
            partition_date = today.strftime('%Y-%m-%d')
            s3_key = f"raw/ingestion_date={partition_date}/stocks_data.parquet"
            
            # Upload para o S3
            s3_client.upload_file(file_path, bucket_name, s3_key)
            print(f"Sucesso! Arquivo salvo em s3://{bucket_name}/{s3_key}")
            
        except Exception as e:
            print(f"Erro na extração: {str(e)}")
            raise e
            
    return {
        'statusCode': 200,
        'body': json.dumps('Extração concluída e salva na RAW.')
    }