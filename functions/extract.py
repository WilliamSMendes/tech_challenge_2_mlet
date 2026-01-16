"""
extract.py - Extracao de dados de acoes brasileiras (Blue Chips B3)
Baixa dados historicos via yfinance e salva em formato Parquet particionado por data.
"""
import json
import os
from pathlib import Path
from datetime import datetime, timedelta

os.environ["HOME"] = "/tmp"
Path("/tmp/.cache/py-yfinance").mkdir(parents=True, exist_ok=True)

import pandas as pd
import yfinance as yf
import pyarrow as pa
import pyarrow.parquet as pq
import boto3

s3_client = boto3.client('s3')

TICKERS_BLUE_CHIPS = [
    'ITUB4.SA',
    'BBDC4.SA',
    'BBAS3.SA'
]


def download_ticker_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Baixa dados historicos de um ticker usando yfinance.
    
    Args:
        ticker: Codigo do ticker (ex: 'PETR4.SA')
        start_date: Data inicial no formato 'YYYY-MM-DD'
        end_date: Data final no formato 'YYYY-MM-DD'
    
    Returns:
        DataFrame com os dados historicos ou DataFrame vazio em caso de erro
    """
    print(f"Baixando dados para {ticker}...")
    
    try:
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
            print(f"  [OK] {len(df)} registros baixados (metodo download)")
            return df
            
    except Exception as e:
        print(f"  [WARN] download falhou: {type(e).__name__}: {str(e)}")
    
    try:
        print(f"  -> Tentando metodo alternativo...")
        ticker_obj = yf.Ticker(ticker)
        df = ticker_obj.history(start=start_date, end=end_date)
        
        if df.empty:
            print(f"  [WARN] Nenhum dado retornado para {ticker}")
            print(f"     Periodo: {start_date} ate {end_date}")
            return pd.DataFrame()
        
        df = df.reset_index()
        df['Ticker'] = ticker
        
        print(f"  [OK] {len(df)} registros baixados (metodo alternativo)")
        return df
        
    except Exception as e:
        print(f"  [ERROR] Erro ao baixar {ticker}: {type(e).__name__}: {str(e)}")
        return pd.DataFrame()


def extract_all_tickers(tickers: list, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Baixa dados de todos os tickers e combina em um unico DataFrame.
    
    Args:
        tickers: Lista de tickers para extrair
        start_date: Data inicial no formato 'YYYY-MM-DD'
        end_date: Data final no formato 'YYYY-MM-DD'
    
    Returns:
        DataFrame consolidado com todos os tickers ou DataFrame vazio
    """
    all_data = []
    
    for ticker in tickers:
        df = download_ticker_data(ticker, start_date, end_date)
        if not df.empty:
            all_data.append(df)
    
    if not all_data:
        print("[ERROR] Nenhum dado foi baixado!")
        return pd.DataFrame()
    
    df_combined = pd.concat(all_data, ignore_index=True)
    
    print(f"\n[OK] Total de {len(df_combined)} registros combinados")
    return df_combined


def save_to_parquet_partitioned(df: pd.DataFrame, output_dir: str):
    """
    Salva DataFrame em formato Parquet particionado por data.
    
    Args:
        df: DataFrame a ser salvo
        output_dir: Diretório local onde salvar os arquivos particionados
    """
    df_copy = df.copy()
    
    if isinstance(df_copy.columns, pd.MultiIndex):
        print("  Achatando MultiIndex...")
        df_copy.columns = ['_'.join(col).strip('_') if col[1] else col[0] 
                           for col in df_copy.columns.values]
    
    print(f"  Colunas: {list(df_copy.columns)}")
    
    df_copy['Date'] = pd.to_datetime(df_copy['Date'])
    df_copy['data_particao'] = df_copy['Date'].dt.strftime('%Y-%m-%d')
    
    particoes = df_copy['data_particao'].unique()
    print(f"  Particoes unicas: {len(particoes)}")
    print(f"  Salvando em: {output_dir}")
    
    # Salva cada partição manualmente sem o prefixo "data_particao="
    for particao in particoes:
        df_particao = df_copy[df_copy['data_particao'] == particao].copy()
        df_particao = df_particao.drop(columns=['data_particao'])
        
        particao_dir = Path(output_dir) / particao
        particao_dir.mkdir(parents=True, exist_ok=True)
        
        arquivo_saida = particao_dir / 'data.parquet'
        df_particao.to_parquet(arquivo_saida, index=False)
        print(f"    -> {particao}: {len(df_particao)} registros")
    
    print(f"  [OK] Dados salvos localmente")

def upload_to_s3(local_dir: str, bucket: str, s3_prefix: str):
    """
    Faz upload de um diretório local particionado para S3, mantendo a estrutura.
    
    Args:
        local_dir: Diretório local contendo os arquivos particionados
        bucket: Nome do bucket S3
        s3_prefix: Prefixo (caminho) no S3 (ex: 'raw')
    """
    try:
        upload_count = 0
        local_path = Path(local_dir)
        
        # Percorre todos os arquivos no diretório particionado
        for file_path in local_path.rglob('*.parquet'):
            # Calcula o caminho relativo para manter a estrutura de partições
            relative_path = file_path.relative_to(local_path)
            s3_key = f"{s3_prefix.strip('/')}/{relative_path.as_posix()}"
            
            print(f"  Uploading: {relative_path} -> s3://{bucket}/{s3_key}")
            
            with open(file_path, 'rb') as f:
                s3_client.put_object(Bucket=bucket, Key=s3_key, Body=f.read())
            
            upload_count += 1
        
        print(f"[OK] {upload_count} arquivos enviados para S3: s3://{bucket}/{s3_prefix}")
        
    except Exception as e:
        print(f"[ERROR] Falha ao enviar para S3: {type(e).__name__}: {str(e)}")
        raise

def lambda_handler(event, context):
    """
    Handler principal da Lambda Function.
    Extrai dados de acoes da B3, salva em Parquet e envia para S3.
    
    Args:
        event: Evento da Lambda (nao utilizado)
        context: Contexto da Lambda
    
    Returns:
        Dict com statusCode e body contendo resultado da execucao
    """
    print("=" * 60)
    print("INICIANDO EXTRACAO DE DADOS - BLUE CHIPS B3")
    print("=" * 60)
    
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=180)
    
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    print(f"\nPeriodo: {start_date_str} ate {end_date_str}")
    print(f"Tickers: {', '.join(TICKERS_BLUE_CHIPS)}")
    
    bucket_name = os.environ.get('BUCKET_NAME', 'meu-bucket-raw')
    print(f"Bucket S3: {bucket_name}\n")
    
    print("[INFO] Testando conectividade com Yahoo Finance...")
    try:
        import socket
        socket.create_connection(("finance.yahoo.com", 443), timeout=5)
        print("[OK] Conectividade OK\n")
    except Exception as e:
        print(f"[WARN] Problema de conectividade: {e}\n")
    
    try:
        print("[INFO] Iniciando download dos tickers...\n")
        df = extract_all_tickers(TICKERS_BLUE_CHIPS, start_date_str, end_date_str)
        
        if df.empty:
            print("\n[WARN] DIAGNOSTICO:")
            print("   - Todos os tickers retornaram vazios")
            print("   - Possiveis causas:")
            print("     * Lambda sem acesso a internet (VPC sem NAT Gateway)")
            print("     * Yahoo Finance bloqueou as requisicoes")
            print("     * Periodo sem dados de mercado")
            print(f"     * Tickers solicitados: {TICKERS_BLUE_CHIPS}")
            
            return {
                'statusCode': 204,
                'body': json.dumps({
                    'message': 'Nenhum dado foi extraido.',
                    'reason': 'Todos os tickers retornaram vazios - possivel problema de conectividade',
                    'period': f"{start_date_str} ate {end_date_str}",
                    'tickers': TICKERS_BLUE_CHIPS
                })
            }
        
        # Salva localmente em formato particionado
        output_dir = '/tmp/raw_data'
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        print("\n[INFO] Salvando dados em formato Parquet particionado...")
        save_to_parquet_partitioned(df, output_dir)
        
        # Upload para S3
        s3_prefix = "raw"
        
        print(f"\n[INFO] Fazendo upload para S3: s3://{bucket_name}/{s3_prefix}")
        upload_to_s3(output_dir, bucket_name, s3_prefix)
        
        #success_key = f"{s3_prefix}/_SUCCESS"
        #s3_client.put_object(Bucket=bucket_name, Key=success_key, Body=b'')
        #print(f"[OK] Marker criado: s3://{bucket_name}/{success_key}")
        
        print("\n" + "=" * 60)
        print("[OK] EXTRACAO CONCLUIDA COM SUCESSO!")
        print("=" * 60)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Extracao concluida com sucesso',
                'records': len(df),
                'tickers': len(df['Ticker'].unique()),
                's3_path': f"s3://{bucket_name}/{s3_prefix}"
            })
        }
        
    except Exception as e:
        print(f"\n[ERROR] ERRO: {str(e)}")
        raise


if __name__ == "__main__":
    os.environ['BUCKET_NAME'] = 'test-bucket'
    
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=180)
    
    df = extract_all_tickers(
        TICKERS_BLUE_CHIPS, 
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d')
    )