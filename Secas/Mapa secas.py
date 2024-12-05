# Databricks notebook source
# MAGIC %md
# MAGIC #### Acessa https://monitordesecas.ana.gov.br/mapa?mes=7&ano=2024 , baixa os arquivos, converte julho24.qgz para geojson com qgis

# COMMAND ----------

import subprocess
import sys

def install_package(package):
    subprocess.check_call([
        sys.executable, "-m", "pip", "install", package,
        "--trusted-host", "pypi.org",
        "--trusted-host", "pypi.python.org",
        "--trusted-host", "files.pythonhosted.org",
        "--no-cache-dir", "--disable-pip-version-check"
    ])


# Lista de pacotes necessários
required_packages = [
    "geopandas", "fastkml", "shapely", "fiona", "pykml", "geopy" , "mlflow", "contextily"
]

# Verifica se os pacotes estão instalados e instala se necessário
installed_packages = subprocess.run([sys.executable, "-m", "pip", "list"], capture_output=True, text=True)
installed_packages = installed_packages.stdout.lower()

for package in required_packages:
    if package.lower() not in installed_packages:
        print(f"Instalando {package}")
        install_package(package)
    else:
        print(f"{package} já está instalado.")

# COMMAND ----------

import geopandas as gpd  
import matplotlib.pyplot as plt  
from shapely import wkt 
from shapely.geometry import shape  
import contextily as ctx  
from fastkml import kml  

# COMMAND ----------

import geopandas as gpd

# Carregar o arquivo GeoJSON exportado
gdf = gpd.read_file("/Workspace/Users/gunar_avila@sicredi.com.br/Não excluir/Finder/Secas/teste3.geojson")

# Verificar se a coluna de severidade está presente
print(gdf.columns)

# Mostrar os primeiros registros
print(gdf.head())


# COMMAND ----------

from pyspark.sql import SparkSession
from shapely.geometry import mapping

# Criar a sessão Spark
spark = SparkSession.builder.getOrCreate()

# Criar uma função para converter os dados do GeoDataFrame para um formato PySpark-friendly
def gdf_to_spark(gdf):
    # Converta geometria para WKT (Well-Known Text)
    gdf['geometry_wkt'] = gdf['geometry'].apply(lambda geom: geom.wkt)
    
    # Selecionar colunas relevantes
    data = gdf[['uf_codigo', 'Valor', 'geometry_wkt']]
    
    # Converter para um DataFrame PySpark
    df_spark = spark.createDataFrame(data)
    
    return df_spark

# Aplicar a função no GeoDataFrame
df_spark = gdf_to_spark(gdf)

# Exibir o DataFrame PySpark
display(df_spark)


# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# Definindo os valores fixos para as colunas
tipo_evento = 'Secas Brasil'
origem = 'monitordesecas.ana.gov.br'
current_date = datetime.now().strftime('%Y-%m-%d')

# Adicionar as colunas 'tipo_evento', 'origem', 'data_run', e 'additional_value' ao DataFrame existente
df_spark = df_spark.withColumn('tipo_evento', F.lit(tipo_evento))
df_spark = df_spark.withColumn('origem', F.lit(origem))
df_spark = df_spark.withColumn('data_run', F.lit(current_date))
df_spark = df_spark.withColumn('additional_value', F.col('uf_codigo'))

# Renomear a coluna 'geometry_wkt' para 'polig' para manter consistência com o outro DataFrame
df_spark = df_spark.withColumnRenamed('geometry_wkt', 'polig')

# Selecionar as colunas na ordem correta
df_spark = df_spark.select('tipo_evento', 'origem', 'polig', 'additional_value', 'data_run')

# Caminho onde o arquivo Parquet será salvo no DBFS
output_path = 'dbfs:/run/Finder/secas/df_polig_secas.parquet'

# Salvar o DataFrame como Parquet no DBFS, sobrescrevendo o arquivo existente
df_spark.write.mode("overwrite").parquet(output_path)

print(f"DataFrame salvo com sucesso em: {output_path}")
