# functions/extract.py
import json
import boto3
import os
from pathlib import Path

# Algumas dependências do yfinance usam SQLite e podem falhar no runtime do Lambda
# por causa da versão do sqlite embutida (ex.: erro perto de "WITHOUT").
# Se pysqlite3-binary estiver no pacote, usamos ele como sqlite3.
try:
    import pysqlite3  # type: ignore
    import sys

    sys.modules["sqlite3"] = pysqlite3
except Exception:
    pass

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
import requests
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from dateutil.relativedelta import relativedelta
import tempfile
import time
from typing import Optional

s3_client = boto3.client('s3')


def _check_outbound_https(url: str, timeout_seconds: float = 3.0) -> tuple[bool, str]:
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=timeout_seconds) as resp:
            return True, f"HTTP {resp.status}"
    except HTTPError as e:
        # HTTPError significa que houve resposta HTTP (conectividade ok),
        # mesmo que o endpoint exija auth/esteja bloqueado.
        return True, f"HTTPError {e.code}: {e.reason}"
    except URLError as e:
        return False, f"URLError: {getattr(e, 'reason', str(e))}"
    except Exception as e:
        return False, f"Exception: {type(e).__name__}: {e}"


def _make_yahoo_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7",
            "Connection": "close",
        }
    )
    return session


def _download_yfinance_with_fallback(
    tickers: list[str],
    start_date: str,
    end_date: str,
    max_attempts: int = 3,
    sleep_seconds: float = 1.5,
) -> pd.DataFrame:
    # Observação: em ambientes AWS, o Yahoo pode bloquear/servir respostas vazias/HTML.
    # Isso pode causar erros como "Expecting value: line 1 column 1" dentro do yfinance.
    # Estratégia:
    # 1) tentar multi-ticker com threads=False (menos agressivo)
    # 2) se vazio, baixar 1 ticker por vez e concatenar
    session = _make_yahoo_session()

    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            df = yf.download(
                tickers=tickers,
                start=start_date,
                end=end_date,
                group_by='ticker',
                progress=False,
                session=session,
                timeout=20,
                threads=False,
            )
            if df is not None and not df.empty:
                return df
            print(f"yfinance multi-ticker retornou vazio (attempt={attempt}/{max_attempts}).")
        except Exception as e:
            last_exc = e
            print(f"yfinance multi-ticker falhou (attempt={attempt}/{max_attempts}): {type(e).__name__}: {e}")
        time.sleep(sleep_seconds)

    print("Tentando fallback: download por ticker (sequencial).")
    frames: list[pd.DataFrame] = []
    for ticker in tickers:
        last_ticker_exc: Optional[Exception] = None
        for attempt in range(1, max_attempts + 1):
            try:
                df_one = yf.download(
                    tickers=ticker,
                    start=start_date,
                    end=end_date,
                    progress=False,
                    session=session,
                    timeout=20,
                    threads=False,
                )
                if df_one is None or df_one.empty:
                    print(f"Ticker {ticker}: retorno vazio (attempt={attempt}/{max_attempts}).")
                else:
                    # Converte colunas para MultiIndex no formato esperado pelo fluxo atual.
                    df_one.columns = pd.MultiIndex.from_product([[ticker], df_one.columns])
                    frames.append(df_one)
                    break
            except Exception as e:
                last_ticker_exc = e
                print(
                    f"Ticker {ticker}: falha (attempt={attempt}/{max_attempts}): {type(e).__name__}: {e}"
                )
            time.sleep(sleep_seconds)

        if last_ticker_exc is not None:
            print(f"Ticker {ticker}: última falha: {type(last_ticker_exc).__name__}: {last_ticker_exc}")

    if not frames:
        if last_exc is not None:
            raise last_exc
        return pd.DataFrame()

    return pd.concat(frames, axis=1).sort_index()

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

    print(f"Período: start_date={start_date} end_date={end_date} (D-1)")
    print(f"Tickers: {tickers}")
    print(f"Bucket RAW: {bucket_name}")

    # Diagnóstico: yfinance precisa de saída HTTPS para Yahoo Finance.
    # Em Lambda dentro de VPC privada sem NAT Gateway (ou sem rota/DNS), o download costuma voltar vazio.
    ok, detail = _check_outbound_https("https://finance.yahoo.com/robots.txt", timeout_seconds=3.0)
    print(f"Outbound HTTPS check (Yahoo): ok={ok} detail={detail}")
    if not ok:
        return {
            'statusCode': 503,
            'body': json.dumps(
                'Sem acesso HTTPS ao Yahoo Finance (provável Lambda em VPC sem NAT/rota/DNS).'
            )
        }
    
    # Cria diretório temporário para salvar o arquivo antes do upload
    with tempfile.TemporaryDirectory() as tmp_dir:
        dataset_dir = os.path.join(tmp_dir, "raw_dataset")
        
        try:
            df = _download_yfinance_with_fallback(
                tickers=tickers,
                start_date=start_date,
                end_date=end_date,
                max_attempts=3,
                sleep_seconds=1.5,
            )
            if df is None or df.empty:
                print("Nenhum dado retornado pelo yfinance; nada para salvar na RAW.")
                try:
                    print(f"yfinance df type={type(df).__name__} shape={getattr(df, 'shape', None)}")
                except Exception:
                    pass
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