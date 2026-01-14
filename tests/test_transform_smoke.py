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
    """Valida se o transform executou sem erros."""
    print(f"\n✅ Validação simplificada...")
    
    # Apenas verifica se algo foi criado (arquivos ou diretórios)
    import os
    items = os.listdir(bucket_path)
    
    # Deve ter pelo menos raw + algo criado pelo transform
    assert len(items) > 1, f"❌ Nenhum output foi gerado (apenas raw existe)"
    
    print(f"  ✓ Transform executou e gerou outputs")
    print(f"  ✓ Arquivos/diretórios criados: {', '.join([i for i in items if i != 'raw'])}")
    
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
