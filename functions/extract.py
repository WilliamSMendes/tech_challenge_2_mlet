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
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
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
        dataset_dir = os.path.join(tmp_dir, "raw_dataset")
        
        try:
            df = yf.download(tickers=tickers, start=start_date, end=end_date, group_by='ticker', progress=False)
            if df is None or df.empty:
                print("Nenhum dado retornado pelo yfinance; nada para salvar na RAW.")
                return {
                    'statusCode': 204,
                    'body': json.dumps('Nenhum dado retornado; extração ignorada.')
                }
            
            # Usa stack e reseta o multiindex para ter virar dados tabulares
            df_stack = df.stack(level=0).reset_index().rename(columns={'level_1': 'Ticker'})

            if df_stack.empty:
                print("DataFrame após stack está vazio; nada para salvar na RAW.")
                return {
                    'statusCode': 204,
                    'body': json.dumps('DataFrame vazio; extração ignorada.')
                }

            # Ajuda a manter schema estável (evita Ticker virar dtype null em casos extremos)
            df_stack['Ticker'] = df_stack['Ticker'].astype('string')

            # Requisito: RAW particionado por data (granularidade diária)
            # Criamos uma coluna de partição diária baseada no Date.
            df_stack['data_pregao'] = pd.to_datetime(df_stack['Date'], errors='coerce').dt.date

            # Escreve dataset parquet particionado por data_pregao (cria pastas data_pregao=YYYY-MM-DD/)
            table = pa.Table.from_pandas(df_stack, preserve_index=False)
            pq.write_to_dataset(table, root_path=dataset_dir, partition_cols=['data_pregao'])

            # Define o prefixo no S3 (organização por dia de ingestão + identificador da execução)
            ingestion_date = today.strftime('%Y-%m-%d')
            run_ts = today.strftime('%H%M%S')
            base_prefix = f"raw/ingestion_date={ingestion_date}/run_ts={run_ts}/"

            # Upload de todos os arquivos parquet do dataset
            uploaded = 0
            for root, _, files in os.walk(dataset_dir):
                for filename in files:
                    if not filename.endswith('.parquet'):
                        continue
                    local_path = os.path.join(root, filename)
                    rel_path = os.path.relpath(local_path, dataset_dir).replace('\\', '/')
                    s3_key = f"{base_prefix}{rel_path}"
                    s3_client.upload_file(local_path, bucket_name, s3_key)
                    uploaded += 1

            # Marker para disparar a trigger apenas uma vez (S3 Notification filtra por _SUCCESS)
            marker_path = os.path.join(tmp_dir, "_SUCCESS")
            Path(marker_path).write_text("ok")
            marker_key = f"{base_prefix}_SUCCESS"
            s3_client.upload_file(marker_path, bucket_name, marker_key)

            print(f"Sucesso! Dataset RAW enviado ({uploaded} arquivos) em s3://{bucket_name}/{base_prefix}")
            print(f"Marker enviado: s3://{bucket_name}/{marker_key}")
            
        except Exception as e:
            print(f"Erro na extração: {str(e)}")
            raise e
            
    return {
        'statusCode': 200,
        'body': json.dumps('Extração concluída e salva na RAW.')
    }