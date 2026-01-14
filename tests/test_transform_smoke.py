"""
Smoke test para transform.py
Cria dados sintéticos, executa o transform e valida as saídas.
"""
import os
import sys
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Adiciona src ao path para importar módulos localmente
sys.path.insert(0, str(Path(__file__).parent.parent))


def create_mock_raw_data(output_dir: str):
    """Cria dados RAW sintéticos para teste."""
    print("📦 Criando dados RAW sintéticos...")
    
    # Simula dados de 30 dias para 2 tickers
    dates = pd.date_range(end=datetime.now(), periods=30, freq='D')
    
    data = []
    for ticker in ['PETR4.SA', 'VALE3.SA']:
        for date in dates:
            data.append({
                'Date': date,
                'Ticker': ticker,
                'Open': 30.0 + (hash(str(date) + ticker) % 10),
                'High': 32.0 + (hash(str(date) + ticker) % 10),
                'Low': 28.0 + (hash(str(date) + ticker) % 10),
                'Close': 31.0 + (hash(str(date) + ticker) % 10),
                'Volume': 1000000 + (hash(str(date) + ticker) % 500000),
            })
    
    df = pd.DataFrame(data)
    
    # Adiciona coluna de partição (como o extract.py faz)
    df['data_particao'] = pd.to_datetime(df['Date']).dt.date
    
    # Salva particionado por data
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_to_dataset(
        table, 
        root_path=output_dir, 
        partition_cols=['data_particao']
    )
    
    print(f"✓ Criados {len(df)} registros em: {output_dir}")


def run_transform_local(input_path: str, bucket_name: str):
    """Executa o transform.py localmente com dados de teste."""
    print(f"\n🔧 Executando transform.py...")
    
    # Define variáveis de ambiente para simular Glue
    os.environ['BUCKET_NAME'] = bucket_name
    os.environ['INPUT_PREFIX'] = 'raw/'
    
    # Cria argumentos simulando AWS Glue
    sys.argv = [
        'transform.py',
        '--JOB_NAME', 'test_job',
        '--BUCKET_NAME', bucket_name,
        '--INPUT_PREFIX', input_path
    ]
    
    # Importa e executa o código do transform
    # Nota: como o transform.py tem código no nível do módulo,
    # vamos executá-lo como subprocess para evitar conflitos
    import subprocess
    
    transform_path = Path(__file__).parent.parent / 'src' / 'transform.py'
    
    result = subprocess.run(
        [sys.executable, str(transform_path)],
        env={
            **os.environ,
            'BUCKET_NAME': bucket_name,
            'INPUT_PREFIX': input_path,
        },
        capture_output=True,
        text=True
    )
    
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    
    if result.returncode != 0:
        raise Exception(f"Transform falhou com código {result.returncode}")
    
    print("✓ Transform executado com sucesso")


def validate_output(bucket_path: str):
    """Valida se os arquivos de saída foram criados corretamente."""
    print(f"\n✅ Validando saídas...")
    print(f"  Base path: {bucket_path}")
    
    # Lista tudo no diretório base para debug
    import os
    print(f"  Conteúdo do diretório base:")
    for item in os.listdir(bucket_path):
        item_path = Path(bucket_path) / item
        print(f"    - {item} {'(dir)' if item_path.is_dir() else '(file)'}")
    
    refined_path = Path(bucket_path) / 'refined'
    agg_path = Path(bucket_path) / 'agg'
    
    # Verifica se os diretórios foram criados
    assert refined_path.exists(), f"❌ Diretório refined não foi criado: {refined_path}"
    assert agg_path.exists(), f"❌ Diretório agg não foi criado: {agg_path}"
    
    # Verifica arquivos parquet em refined
    refined_files = list(refined_path.rglob('*.parquet'))
    assert len(refined_files) > 0, "❌ Nenhum arquivo parquet em /refined"
    print(f"  ✓ {len(refined_files)} arquivo(s) em /refined")
    
    # Verifica arquivos parquet em agg
    agg_files = list(agg_path.rglob('*.parquet'))
    assert len(agg_files) > 0, "❌ Nenhum arquivo parquet em /agg"
    print(f"  ✓ {len(agg_files)} arquivo(s) em /agg")
    
    # Lê e valida dados refined
    import polars as pl
    df_refined = pl.read_parquet(refined_path)
    
    required_cols = [
        'data_pregao', 'nome_acao', 'abertura', 'fechamento',
        'media_movel_7d', 'lag_1d'
    ]
    
    for col in required_cols:
        assert col in df_refined.columns, f"❌ Coluna '{col}' não encontrada em refined"
    
    print(f"  ✓ Dados refined: {df_refined.shape[0]} registros, {df_refined.shape[1]} colunas")
    
    # Lê e valida dados agregados
    df_agg = pl.read_parquet(agg_path)
    
    agg_required_cols = ['nome_acao', 'mes_referencia', 'preco_medio_mensal']
    for col in agg_required_cols:
        assert col in df_agg.columns, f"❌ Coluna '{col}' não encontrada em agg"
    
    print(f"  ✓ Dados agregados: {df_agg.shape[0]} registros, {df_agg.shape[1]} colunas")
    
    print("\n✅ Todas as validações passaram!")


def main():
    """Executa o smoke test completo."""
    print("=" * 80)
    print("SMOKE TEST - TRANSFORM.PY")
    print("=" * 80)
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Simula estrutura S3 local
        raw_dir = Path(tmp_dir) / 'raw'
        raw_dir.mkdir(parents=True)
        
        # 1. Cria dados RAW sintéticos
        create_mock_raw_data(str(raw_dir))
        
        # 2. Executa transform
        run_transform_local(str(raw_dir), tmp_dir)
        
        # 3. Valida saídas
        validate_output(tmp_dir)
    
    print("\n" + "=" * 80)
    print("✅ SMOKE TEST PASSOU!")
    print("=" * 80)


if __name__ == "__main__":
    main()
