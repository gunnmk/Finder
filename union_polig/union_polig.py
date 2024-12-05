# Databricks notebook source
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
    "geopandas", "fastkml", "shapely", "fiona", "pykml", "geopy" , "mlflow", "contextily", "geobr"
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

from pyspark.sql import SparkSession

# Caminhos corretos dos arquivos Parquet no DBFS
path_enchentes = 'dbfs:/run/Finder/enchentes/df_polig_enchentes.parquet'
path_secas = 'dbfs:/run/Finder/secas/df_polig_secas.parquet'
path_queimadas = 'dbfs:/run/Finder/queimadas/df_polig_queimadas.parquet'

# Carregar os DataFrames a partir dos arquivos Parquet
df_enchentes = spark.read.parquet(path_enchentes)
df_secas = spark.read.parquet(path_secas)
df_queimadas = spark.read.parquet(path_queimadas)

# Realizar o union dos DataFrames
df_union = df_enchentes.union(df_secas).union(df_queimadas)

# Mostrar o DataFrame unido
display(df_union)


# COMMAND ----------

from shapely import wkt
from shapely.geometry import Polygon, MultiPolygon
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Função para converter os diferentes formatos de polígono para POLYGON 2D
def convert_to_polygon(wkt_str):
    geom = wkt.loads(wkt_str)
    
    # Se for MULTIPOLYGON, converter para POLYGON usando o primeiro polígono
    if isinstance(geom, MultiPolygon):
        geom = geom.geoms[0]  # Usar o primeiro polígono do MultiPolygon
    
    # Se for POLYGON Z, remover a terceira dimensão
    if geom.has_z:
        geom = Polygon([(x, y) for x, y, z in geom.exterior.coords])
    
    return geom.wkt

# Registrar a UDF no Spark
convert_to_polygon_udf = F.udf(convert_to_polygon, StringType())

# Aplicar a função para padronizar os polígonos no formato POLYGON
df_union_standardized = df_union.withColumn("polig", convert_to_polygon_udf(F.col("polig")))

# Mostrar os primeiros registros para confirmar a conversão
display(df_union_standardized)


# COMMAND ----------

# Filtrar o DataFrame para excluir as linhas onde 'additional_value' é igual a 'si'
df_union_standardized = df_union_standardized.filter(df_union_standardized['additional_value'] != 'si')
display(df_union_standardized)

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit

# Criar a nova coluna concatenando as colunas desejadas
df_union_standardized = df_union_standardized.withColumn(
    'id_polig_evento',
    concat(
        col('tipo_evento'), lit('_'),
        col('origem'), lit('_'),
        col('polig'), lit('_'),
        col('additional_value'), lit('_'),
        col('data_run')
    )
)

# Exibir o DataFrame para verificar o resultado
display(df_union_standardized)


# COMMAND ----------

# Caminho do diretório no DBFS
output_path = "dbfs:/run/Finder/union/df_union_standardized"

# Salvando o DataFrame como Parquet com modo overwrite
df_union_standardized.write.mode("overwrite").parquet(output_path)


# COMMAND ----------

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as ctx
from shapely import wkt
import unicodedata
from matplotlib.patches import Patch

# Converter PySpark DataFrame para Pandas DataFrame
df_pandas = df_union_standardized.toPandas()

# Converter a coluna 'polig' de WKT para geometria
df_pandas['geometry'] = df_pandas['polig'].apply(wkt.loads)
gdf = gpd.GeoDataFrame(df_pandas, geometry='geometry')

# Definir o CRS inicial (assumindo que está em WGS84)
gdf.crs = 'EPSG:4326'

# Função para normalizar o texto
def normalize_text(text):
    text = str(text)
    text = text.strip()
    text = text.lower()
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
    return text

# Aplicar a normalização à coluna 'tipo_evento'
gdf['tipo_evento_normalizado'] = gdf['tipo_evento'].apply(normalize_text)

# Definir os nomes normalizados dos eventos
drought_event_name = 'secas brasil'
fire_event_name = 'incendios brasil'
flood_event_name = 'enchentes rs'

# Separar os grupos de eventos usando a coluna normalizada
droughts = gdf[gdf['tipo_evento_normalizado'] == drought_event_name].copy()
fires = gdf[gdf['tipo_evento_normalizado'] == fire_event_name].copy()
floods = gdf[gdf['tipo_evento_normalizado'] == flood_event_name].copy()

# Verificar se os DataFrames estão vazios
print(f"Número de polígonos de secas: {len(droughts)}")
print(f"Número de polígonos de incêndios: {len(fires)}")
print(f"Número de polígonos de enchentes: {len(floods)}")

# Corrigir geometrias inválidas e preparar os DataFrames
for df in [droughts, fires, floods]:
    if not df.empty:
        invalid_geometries = df[~df['geometry'].is_valid]
        if not invalid_geometries.empty:
            df['geometry'] = df['geometry'].buffer(0)
            print(f"Geometrias inválidas corrigidas para {df['tipo_evento_normalizado'].iloc[0]}")
        df.to_crs(epsg=3857, inplace=True)

# Mapear as severidades para as cores especificadas em 'droughts'
severity_colors = {
    's0': '#ffeda0',  # Amarelo claro
    's1': '#feb24c',  # Laranja claro
    's2': '#fd8d3c',  # Laranja forte
    's3': '#fc4e2a',  # Vermelho claro
    's4': '#b10026'   # Vermelho escuro
}
droughts['additional_value_normalizado'] = droughts['additional_value'].apply(normalize_text)
droughts['color'] = droughts['additional_value_normalizado'].map(severity_colors)
fires['color'] = '#FF0000'  # Vermelho sangue
floods['color'] = 'blue'    # Azul

# Carregar os limites dos estados do Brasil a partir do arquivo KML
kml_path = '/Workspace/Finder/union_polig/Brasil_Estados.kml'

# Carregar o KML usando GeoPandas
try:
    states_gdf = gpd.read_file(kml_path, driver='KML')
    print(f"Número de entidades no GeoDataFrame dos estados: {len(states_gdf)}")
except Exception as e:
    print(f"Erro ao carregar o arquivo KML: {e}")
    states_gdf = gpd.GeoDataFrame()  # Cria um GeoDataFrame vazio em caso de erro

# Verificar e definir o CRS
if not states_gdf.empty:
    if states_gdf.crs is None:
        states_gdf.set_crs(epsg=4326, inplace=True)
        print("CRS dos estados definido para EPSG:4326 (WGS84).")
    else:
        print(f"CRS original dos estados: {states_gdf.crs}")
    
    # Transformar para CRS 3857 para coincidir com as outras camadas
    states_gdf = states_gdf.to_crs(epsg=3857)
    print("CRS dos estados transformado para EPSG:3857.")
else:
    print("GeoDataFrame dos estados está vazio. Verifique o arquivo KML.")

# Combinar Geometrias para Determinar os Limites do Mapa
if not states_gdf.empty:
    combined_geometries = pd.concat(
        [droughts.geometry, fires.geometry, floods.geometry, states_gdf.geometry],
        ignore_index=True
    )
else:
    combined_geometries = pd.concat(
        [droughts.geometry, fires.geometry, floods.geometry],
        ignore_index=True
    )

xmin, ymin, xmax, ymax = combined_geometries.total_bounds
print(f"Limites do mapa ajustados para: ({xmin}, {ymin}, {xmax}, {ymax})")

# Criar a figura e os eixos
fig, ax = plt.subplots(figsize=(15, 15))

# Definir os limites do mapa
ax.set_xlim(xmin, xmax)
ax.set_ylim(ymin, ymax)
print(f"Limites do mapa definidos para: ({xmin}, {ymin}, {xmax}, {ymax})")

# Adicionar o mapa de fundo do OpenStreetMap com nível de zoom fixo
# Escolher um zoom apropriado para o Brasil, por exemplo, 5
ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, zoom=5)

# Plotar as camadas de eventos
if not droughts.empty:
    droughts.plot(
        ax=ax,
        color=droughts['color'],
        edgecolor=droughts['color'],
        linewidth=0.5,
        zorder=3,
        alpha=0.6
    )

if not fires.empty:
    fires.plot(
        ax=ax,
        color=fires['color'],
        edgecolor=fires['color'],
        linewidth=0.5,
        zorder=4
    )

if not floods.empty:
    floods.plot(
        ax=ax,
        color=floods['color'],
        edgecolor=floods['color'],
        linewidth=0.5,
        zorder=5
    )

# Plotar os limites dos estados sobre as outras camadas
if not states_gdf.empty:
    states_gdf.plot(
        ax=ax,
        edgecolor='black',
        facecolor='none',
        linewidth=1.5,  # Aumentar a largura para melhor visibilidade
        zorder=6
    )
    print("Limites dos estados plotados.")
else:
    print("Não há limites de estados para plotar.")

# Ajustar as legendas com os novos labels
legend_elements = [
    Patch(facecolor=severity_colors['s0'], edgecolor=severity_colors['s0'], label='Seca com severidade 0', alpha=0.6),
    Patch(facecolor=severity_colors['s1'], edgecolor=severity_colors['s1'], label='Seca com severidade 1', alpha=0.6),
    Patch(facecolor=severity_colors['s2'], edgecolor=severity_colors['s2'], label='Seca com severidade 2', alpha=0.6),
    Patch(facecolor=severity_colors['s3'], edgecolor=severity_colors['s3'], label='Seca com severidade 3', alpha=0.6),
    Patch(facecolor=severity_colors['s4'], edgecolor=severity_colors['s4'], label='Seca com severidade 4', alpha=0.6),
    Patch(facecolor='#FF0000', edgecolor='#FF0000', label='Incêndios'),
    Patch(facecolor='blue', edgecolor='blue', label='Enchentes RS')
]

# Adicionar a legenda ao gráfico no canto superior direito
ax.legend(
    handles=legend_elements,
    title='Legenda',
    loc='upper right'
)

# Remover os eixos para clareza
ax.set_axis_off()

# Manter o aspecto igual
ax.set_aspect('equal')

# Mostrar o mapa
plt.show()
