import os
import subprocess
import shutil
import json
import glob       
import pyarrow as pa
import pyarrow.parquet as pq   
import pandas as pd
import get_fake_event
import get_data
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from time import time

## Escolha do tipo de formato de saida ( CSV / JSON / PARQUET)
# tipo_saida = 'csv'
# tipo_saida = 'json'
tipo_saida = 'parquet'

# Criação da função para calcular o intervalo entre execuções
t_a = datetime.now()
t_b = datetime.now()

def diff(t_a, t_b):
    t_diff = relativedelta(t_b, t_a)  # later/end time comes first!
    return '{h}h {m}m {s}s'.format(h=t_diff.hours, m=t_diff.minutes, s=t_diff.seconds)

t_b = datetime.now()
print(datetime.now(), f' |   [Inicio da carga]   | Finalizado -- {diff(t_a, t_b)}')

t_b = datetime.now()
print(datetime.now(), f' |   [Importação das bibliotecas]    | Finalizado -- {diff(t_a, t_b)}')

# Variáveis de data e hora
ref_data = datetime.now()
ref_year = datetime.now().year
ref_time = datetime.now().strftime("%H%M%S")

t_b = datetime.now()
print(datetime.now(), f' |   [Definição das variáveis de tempo]  | Finalizado -- {diff(t_a, t_b)}')

# Gera a lista dos dados
events_fake = get_fake_event.create_data(10000)

t_b = datetime.now()
print(datetime.now(), f' |   [Criação da base fake]  | Finalizado -- {diff(t_a, t_b)}')

# Converte em DataFrame
df = pd.DataFrame.from_dict(events_fake).transpose()

t_b = datetime.now()
print(datetime.now(), f' |   [Criação do dataset pandas] | Finalizado -- {diff(t_a, t_b)}')

# Realiza a conversa do dataset em pandas para uma table
table = pa.Table.from_pandas(df)

t_b = datetime.now()
print(datetime.now(), f' |   [Conversão do dataset pandas em table]  | Finalizado -- {diff(t_a, t_b)}')

## Gera o arquivo JSON na pasta temporária ##
# Declara as variáveis de dia e hora corrente para montar o nome do arquivo
data = datetime.now().strftime('%Y%m%d')
tempo = str(ref_time)

# Pasta e nome do arquivo no diretório temporário
filename = f'C:/Temp/stage/fake_events_{data}_{tempo}'

t_b = datetime.now()
print(datetime.now(), f' |   [Gravação na pasta temporária]  | Finalizado -- {diff(t_a, t_b)}')

# # Realiza a conversão da table para o formato de saída ( .csv / .json / .parquet ) 
if tipo_saida == 'csv':
    # Convert a table para o arquivo .csv
    df.to_csv(filename+'.csv')
elif tipo_saida == 'json':
    # Convert a table para o arquivo .json
    df.to_json(filename+'.json')
else:
    # Convert a table para o arquivo .parquet
    pq.write_table(table, filename+'.snappy.parquet', compression='snappy')

t_b = datetime.now()
if tipo_saida == 'csv':
    print(datetime.now(), f' |   [Conversão do table em arquivo csv] | Finalizado -- {diff(t_a, t_b)}')
elif tipo_saida == 'json':
    print(datetime.now(), f' |   [Conversão do table em arquivo json] | Finalizado -- {diff(t_a, t_b)}')
else:
    print(datetime.now(), f' |   [Conversão do table em arquivo parquet] | Finalizado -- {diff(t_a, t_b)}')

## Input no HDFS ##
# Caminho de origem do arquivo .parquet / .csv
source_dir = 'C:\Temp\stage'

if tipo_saida == 'csv':
    sources = glob.glob(os.path.join(source_dir,"*.csv"))
elif tipo_saida == 'json':
    sources = glob.glob(os.path.join(source_dir,"*.json"))
else:
    sources = glob.glob(os.path.join(source_dir,"*.parquet"))

# Criação do comando shell para realização do put
cmd = f'hadoop fs -put {sources[0]} /datalake/fake-events/raw/'
# Comando put no hdfs
os.system(cmd)

t_b = datetime.now()
print(datetime.now(), f' |   [Realização do PUT no HDFS] | Finalizado -- {diff(t_a, t_b)}')

## Limpeza da Pasta Temporaria ##
# Rotina para limpar a pasta Temp após o input no HDFS
source_dir = 'C:\Temp\stage'

if tipo_saida == 'csv':
    sources = glob.glob(os.path.join(source_dir,"*.csv"))
elif tipo_saida == 'json':
    sources = glob.glob(os.path.join(source_dir,"*.json"))
else:
    sources = glob.glob(os.path.join(source_dir,"*.parquet"))

for f in sources:
    os.remove(f)

t_b = datetime.now()
print(datetime.now(), f' |   [Limpeza da pasta temporária]   | Finalizado -- {diff(t_a, t_b)}')

t_b = datetime.now()
print(datetime.now(), f' |   [Fim da carga]  | Finalizado -- {diff(t_a, t_b)}')