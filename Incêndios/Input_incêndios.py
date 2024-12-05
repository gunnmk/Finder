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


required_packages = [
    "geopandas", "fastkml", "shapely", "fiona", "pykml", "geopy" , "mlflow", "contextily"
]

installed_packages = subprocess.run([sys.executable, "-m", "pip", "list"], capture_output=True, text=True)
installed_packages = installed_packages.stdout.lower()

for package in required_packages:
    if package.lower() not in installed_packages:
        print(f"Instalando {package}")
        install_package(package)
    else:
        print(f"{package} já está instalado.")

# COMMAND ----------

import urllib.request

# URL do arquivo KML
url = 'https://panorama.sipam.gov.br/geoserver/painel_do_fogo/ows?service=WFS&version=2.0.0&request=GetFeature&typeName=painel_do_fogo:mv_evento_filtro_exportKML&maxFeatures=1000000&outputFormat=kml'

# Caminho temporário para salvar o arquivo localmente
local_path = "/tmp/temp_kml_file.kml"

# Fazer o download do arquivo KML
urllib.request.urlretrieve(url, local_path)

# Caminho no DBFS onde você quer salvar o arquivo
dbfs_path = "dbfs:/run/Finder/queimadas/evento_fogo.kml"

# Copiar o arquivo do sistema local para o DBFS
dbutils.fs.cp("file:" + local_path, dbfs_path)

print("Arquivo KML baixado e salvo no DBFS com sucesso!")


# COMMAND ----------

from fastkml import kml
from shapely.geometry import shape
import matplotlib.pyplot as plt

# Função para explorar as features do KML
def explore_kml_features_detailed(features, level=0):
    for feature in features:
        indent = '  ' * level  # Indentação para visualizar a hierarquia
        print(f"{indent}Feature Type: {type(feature).__name__}")
        print(f"{indent}Name: {feature.name}")
        print(f"{indent}Description: {feature.description}")
        if hasattr(feature, 'geometry'):
            print(f"{indent}Geometry Type: {type(feature.geometry).__name__}")
            if feature.geometry:
                print(f"{indent}Geometry WKT: {feature.geometry.wkt if feature.geometry else 'No Geometry'}")
        else:
            print(f"{indent}No geometry attribute found.")
        print(f"{indent}----------------")
        
        # Se o feature contém sub-features, explorar recursivamente
        if hasattr(feature, 'features'):
            explore_kml_features_detailed(feature.features(), level + 1)

# Caminho do arquivo KML
kml_path = '/dbfs/run/Finder/queimadas/evento_fogo.kml'

# Ler o arquivo KML
k = kml.KML()
with open(kml_path, 'rb') as file:
    doc = file.read()
    k.from_string(doc)

# Explorar todas as features do KML
all_features = list(k.features())
explore_kml_features_detailed(all_features)


# COMMAND ----------

from shapely import wkt

# Função para explorar as geometrias e depurar
def explore_kml_features_detailed(features, level=0):
    polygons_found = 0
    for feature in features:
        # Se o feature contém sub-features, explorar recursivamente
        if hasattr(feature, 'features'):
            explore_kml_features_detailed(feature.features(), level + 1)
        
        if hasattr(feature, 'geometry') and feature.geometry:
            try:
                # Tentar extrair e carregar a geometria
                geometry_wkt = feature.geometry.wkt
                print(f"Geometry WKT encontrada: {geometry_wkt[:100]}...")  # Mostra os primeiros 100 caracteres da WKT
                
                # Usar Shapely para tentar carregar a geometria
                polygon = wkt.loads(geometry_wkt)
                if polygon.is_valid:
                    polygons_found += 1
                    print(f"Polígono {polygons_found} encontrado com sucesso.")
                else:
                    print(f"Geometria inválida: {geometry_wkt[:100]}...")
                    
            except Exception as e:
                print(f"Erro ao processar a geometria: {e}")
    
    if polygons_found == 0:
        print("Nenhum polígono válido encontrado.")
    else:
        print(f"Total de polígonos válidos encontrados: {polygons_found}")

# Chamar a função para depuração detalhada das features
explore_kml_features_detailed(all_features)


# COMMAND ----------

import matplotlib.pyplot as plt
from shapely import wkt

# Função para extrair e acumular polígonos
def collect_valid_polygons(features, polygons_list):
    for feature in features:
        # Se o feature contém sub-features, explorar recursivamente
        if hasattr(feature, 'features'):
            collect_valid_polygons(feature.features(), polygons_list)  # Explora recursivamente
            
        if hasattr(feature, 'geometry') and feature.geometry:
            try:
                # Extrair o WKT da geometria
                geometry_wkt = feature.geometry.wkt
                polygon = wkt.loads(geometry_wkt)  # Usar o Shapely para carregar a geometria
                
                if polygon.is_valid:
                    polygons_list.append(polygon)  # Adicionar polígono à lista

            except Exception as e:
                print(f"Erro ao processar a geometria: {e}")

# Lista para armazenar todos os polígonos válidos
polygons_list = []

# Coletar todos os polígonos válidos
collect_valid_polygons(all_features, polygons_list)

# Plotar os polígonos apenas uma vez
if len(polygons_list) > 0:
    fig, ax = plt.subplots(figsize=(12, 12))
    for polygon in polygons_list:
        x, y = polygon.exterior.xy  # Extrair as coordenadas
        ax.plot(x, y)  # Plotar o polígono
    ax.set_title(f"Polígonos do KML ({len(polygons_list)} polígonos)")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    plt.show()
else:
    print("Nenhum polígono válido encontrado.")


# COMMAND ----------

import geopandas as gpd
import matplotlib.pyplot as plt
from shapely import wkt
import contextily as ctx

# Função para coletar os polígonos em um GeoDataFrame
def collect_polygons_to_gdf(features):
    polygons = []
    for feature in features:
        # Se o feature contém sub-features, explorar recursivamente
        if hasattr(feature, 'features'):
            polygons.extend(collect_polygons_to_gdf(feature.features()))  # Explora recursivamente
            
        if hasattr(feature, 'geometry') and feature.geometry:
            try:
                # Extrair o WKT da geometria
                geometry_wkt = feature.geometry.wkt
                polygon = wkt.loads(geometry_wkt)  # Usar o Shapely para carregar a geometria
                
                if polygon.is_valid:
                    polygons.append(polygon)  # Adicionar o polígono à lista

            except Exception as e:
                print(f"Erro ao processar a geometria: {e}")
    return polygons

# Coletar os polígonos em um GeoDataFrame
polygons = collect_polygons_to_gdf(all_features)

# Criar um GeoDataFrame a partir dos polígonos coletados
gdf = gpd.GeoDataFrame(geometry=polygons, crs='EPSG:4326')

# Plotar os polígonos com um mapa de fundo
fig, ax = plt.subplots(figsize=(12, 12))

# Plotar o GeoDataFrame com os polígonos
gdf.plot(ax=ax, alpha=0.5, edgecolor='k')

# Adicionar mapa de fundo (tiles) do OpenStreetMap
ctx.add_basemap(ax, crs=gdf.crs.to_string(), source=ctx.providers.OpenStreetMap.Mapnik)

ax.set_title(f"Polígonos do KML com mapa de fundo ({len(polygons)} polígonos)")
ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")

plt.show()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from shapely import wkt



# Função para extrair e acumular polígonos válidos
def collect_valid_polygons(features):
    polygons_list = []
    for feature in features:
        # Se o feature contém sub-features, explorar recursivamente
        if hasattr(feature, 'features'):
            polygons_list.extend(collect_valid_polygons(feature.features()))  # Explora recursivamente
            
        if hasattr(feature, 'geometry') and feature.geometry:
            try:
                # Extrair o WKT da geometria
                geometry_wkt = feature.geometry.wkt
                polygon = wkt.loads(geometry_wkt)  # Usar o Shapely para carregar a geometria
                
                if polygon.is_valid:
                    polygons_list.append(polygon.wkt)  # Adicionar polígono válido à lista como WKT

            except Exception as e:
                print(f"Erro ao processar a geometria: {e}")
    return polygons_list

# Coletar todos os polígonos válidos
polygons_list = collect_valid_polygons(all_features)

# Verificar se a lista está vazia
if polygons_list:
    # Definindo os valores fixos para as colunas
    tipo_evento = 'Incêndios Brasil'
    origem = 'panorama.sipam.gov.br/geoserver/painel_do_fogo'
    current_date = datetime.now().strftime('%Y-%m-%d')
    additional_value = ' '  # Valor em branco para a coluna additional_value
    
    # Criando uma lista de tuplas para cada polígono com os valores das colunas
    polygons_data = [(tipo_evento, origem, polig, additional_value, current_date) for polig in polygons_list]
    
    # Criando um DataFrame PySpark a partir da lista de tuplas
    df = spark.createDataFrame(polygons_data, ["tipo_evento", "origem", "polig", "additional_value", "data_run"])
    
    display(df)

    # Caminho onde o arquivo Parquet será salvo no DBFS
    output_path = 'dbfs:/run/Finder/queimadas/df_polig_queimadas.parquet'
    
    # Salvar o DataFrame como Parquet no DBFS com modo overwrite
    df.write.mode("overwrite").parquet(output_path)
    
    print(f"DataFrame salvo com sucesso em: {output_path}")
else:
    print("Nenhum polígono válido encontrado.")

