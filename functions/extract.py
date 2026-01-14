"""
extract.py - Extração de dados de ações brasileiras (Blue Chips B3)
Baixa dados históricos via yfinance e salva em formato Parquet particionado por data.
"""
import json
import os
from pathlib import Path
from datetime import datetime, timedelta

# Lambda: redireciona HOME para /tmp (único diretório gravável)
# Cria diretório de cache antes de importar yfinance
os.environ["HOME"] = "/tmp"
Path("/tmp/.cache/py-yfinance").mkdir(parents=True, exist_ok=True)

import pandas as pd
import yfinance as yf
import pyarrow as pa
import pyarrow.parquet as pq
import boto3

# Cliente S3
s3_client = boto3.client('s3')

# Lista de tickers - Bancos Blue Chips B3
TICKERS_BLUE_CHIPS = [
    'ITUB4.SA',  # Itaú
    'BBDC4.SA',  # Bradesco
    'BBAS3.SA'   # Banco do Brasil
]


def download_ticker_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Baixa dados históricos de um ticker usando yfinance.
    
    Args:
        ticker: Código do ticker (ex: 'PETR4.SA')
        start_date: Data inicial no formato 'YYYY-MM-DD'
        end_date: Data final no formato 'YYYY-MM-DD'
    
    Returns:
        DataFrame com os dados históricos
    """
    print(f"Baixando dados para {ticker}...")
    
    try:
        # Tenta método 1: yf.download (mais rápido)
        df = yf.download(
            ticker, 
            start=start_date, 
            end=end_date, 
            progress=False,
            timeout=10
        )
        
        if not df.empty:
            df = df.reset_index()
            df['Ticker'] = ticker
            print(f"  ✓ {len(df)} registros baixados (método download)")
            return df
            
    except Exception as e:
        print(f"  ⚠ download falhou: {type(e).__name__}: {str(e)}")
    
    try:
        # Tenta método 2: Ticker().history() (mais robusto)
        print(f"  → Tentando método alternativo...")
        ticker_obj = yf.Ticker(ticker)
        df = ticker_obj.history(start=start_date, end=end_date)
        
        if df.empty:
            print(f"  ⚠ Nenhum dado retornado para {ticker}")
            print(f"     Período: {start_date} até {end_date}")
            return pd.DataFrame()
        
        # Reset index para transformar Date em coluna
        df = df.reset_index()
        
        # Adiciona coluna com o ticker
        df['Ticker'] = ticker
        
        print(f"  ✓ {len(df)} registros baixados (método alternativo)")
        return df
        
    except Exception as e:
        print(f"  ✗ Erro ao baixar {ticker}: {type(e).__name__}: {str(e)}")
        return pd.DataFrame()


def extract_all_tickers(tickers: list, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Baixa dados de todos os tickers e combina em um único DataFrame.
    
    Args:
        tickers: Lista de tickers
        start_date: Data inicial
        end_date: Data final
    
    Returns:
        DataFrame consolidado com todos os tickers
    """
    all_data = []
    
    for ticker in tickers:
        df = download_ticker_data(ticker, start_date, end_date)
        if not df.empty:
            all_data.append(df)
    
    if not all_data:
        print("❌ Nenhum dado foi baixado!")
        return pd.DataFrame()
    
    # Combina todos os DataFrames
    df_combined = pd.concat(all_data, ignore_index=True)
    
    print(f"\n✓ Total de {len(df_combined)} registros combinados")
    return df_combined


def save_to_parquet_partitioned(df: pd.DataFrame, output_dir: str):
    """
    Salva DataFrame em formato Parquet particionado por data.
    
    Args:
        df: DataFrame a ser salvo
        output_dir: Diretório de saída
    """
    # Faz uma cópia para não modificar o original
    df_copy = df.copy()
    
    # Cria coluna de partição (apenas data, sem hora)
    # Garante que Date seja datetime
    df_copy['Date'] = pd.to_datetime(df_copy['Date'])
    df_copy['data_particao'] = df_copy['Date'].dt.date.astype(str)
    
    # Converte para PyArrow Table
    table = pa.Table.from_pandas(df_copy, preserve_index=False)
    
    # Salva particionado por data
    pq.write_to_dataset(
        table, 
        root_path=output_dir, 
        partition_cols=['data_particao'],
        existing_data_behavior='overwrite_or_ignore'
    )
    
    print(f"✓ Dados salvos em: {output_dir}")
    print(f"  Partições por data: {df_copy['data_particao'].nunique()}")


def upload_to_s3(local_dir: str, bucket: str, s3_prefix: str):
    """
    Faz upload de um diretório local para S3.
    
    Args:
        local_dir: Diretório local
        bucket: Nome do bucket S3
        s3_prefix: Prefixo (caminho) no S3
    """
    uploaded_count = 0
    
    for root, _, files in os.walk(local_dir):
        for filename in files:
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, local_dir)
            s3_key = f"{s3_prefix}/{relative_path}".replace('\\', '/')
            
            print(f"  Uploading: {s3_key}")
            s3_client.upload_file(local_path, bucket, s3_key)
            uploaded_count += 1
    
    print(f"✓ {uploaded_count} arquivos enviados para S3")


def lambda_handler(event, context):
    """
    Handler principal da Lambda Function.
    """
    print("=" * 60)
    print("INICIANDO EXTRAÇÃO DE DADOS - BLUE CHIPS B3")
    print("=" * 60)
    
    # Configuração de período (últimos 6 meses)
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=180)
    
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    print(f"\nPeríodo: {start_date_str} até {end_date_str}")
    print(f"Tickers: {', '.join(TICKERS_BLUE_CHIPS)}")
    
    # Bucket S3
    bucket_name = os.environ.get('BUCKET_NAME', 'meu-bucket-raw')
    print(f"Bucket S3: {bucket_name}\n")
    
    # Teste de conectividade Yahoo Finance
    print("🔍 Testando conectividade com Yahoo Finance...")
    try:
        import socket
        socket.create_connection(("finance.yahoo.com", 443), timeout=5)
        print("✓ Conectividade OK\n")
    except Exception as e:
        print(f"⚠️  Problema de conectividade: {e}\n")
    
    try:
        # 1. Baixar dados
        print("🚀 Iniciando download dos tickers...\n")
        df = extract_all_tickers(TICKERS_BLUE_CHIPS, start_date_str, end_date_str)
        
        if df.empty:
            print("\n⚠️  DIAGNÓSTICO:")
            print("   • Todos os tickers retornaram vazios")
            print("   • Possíveis causas:")
            print("     - Lambda sem acesso à internet (VPC sem NAT Gateway)")
            print("     - Yahoo Finance bloqueou as requisições")
            print("     - Período sem dados de mercado")
            print(f"     - Tickers solicitados: {TICKERS_BLUE_CHIPS}")
            
            return {
                'statusCode': 204,
                'body': json.dumps({
                    'message': 'Nenhum dado foi extraído.',
                    'reason': 'Todos os tickers retornaram vazios - possível problema de conectividade',
                    'period': f"{start_date_str} até {end_date_str}",
                    'tickers': TICKERS_BLUE_CHIPS
                })
            }
        
        # 2. Salvar localmente em /tmp (Lambda tem acesso a /tmp)
        output_dir = '/tmp/raw_data'
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        save_to_parquet_partitioned(df, output_dir)
        
        # 3. Upload para S3
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        s3_prefix = f"raw/execution_date={datetime.now().strftime('%Y-%m-%d')}/run_{timestamp}"
        
        print(f"\nFazendo upload para S3: s3://{bucket_name}/{s3_prefix}")
        upload_to_s3(output_dir, bucket_name, s3_prefix)
        
        print("\n" + "=" * 60)
        print("✓ EXTRAÇÃO CONCLUÍDA COM SUCESSO!")
        print("=" * 60)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Extração concluída com sucesso',
                'records': len(df),
                'tickers': len(df['Ticker'].unique()),
                's3_path': f"s3://{bucket_name}/{s3_prefix}"
            })
        }
        
    except Exception as e:
        print(f"\n❌ ERRO: {str(e)}")
        raise


# Para teste local
if __name__ == "__main__":
    # Simula execução local (sem S3)
    os.environ['BUCKET_NAME'] = 'test-bucket'
    
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=180)
    
    df = extract_all_tickers(
        TICKERS_BLUE_CHIPS, 
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d')
    )
    
    if not df.empty:
        output_dir = './raw'
        save_to_parquet_partitioned(df, output_dir)
        print(f"\n✓ Dados salvos localmente em: {output_dir}")