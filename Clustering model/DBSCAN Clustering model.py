# Databricks notebook source
import os
import pandas as pd
from sklearn.cluster import DBSCAN
from sklearn.impute import SimpleImputer
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

# Configuração de threads para o scikit-learn
os.environ['OMP_NUM_THREADS'] = '16'

# Inicializando SparkSession
spark = SparkSession.builder.appName("DBSCAN Clustering").getOrCreate()

# Carrega o DataFrame
df = spark.sql("""
    SELECT user_id, cast(latitude as float) as latitude, cast(longitude as float) as longitude
""")

def apply_dbscan(partition):
    # Transforma a partição em um DataFrame do Pandas
    pdf = pd.DataFrame(list(partition), columns=['user_id', 'latitude', 'longitude'])
    if pdf.empty:
        return []
    
    results = []
    grouped = pdf.groupby('user_id')
    
    # Aplica DBSCAN a cada grupo de conta
    for name, group in grouped:
        imputer = SimpleImputer(strategy='mean')
        group[['latitude', 'longitude']] = imputer.fit_transform(group[['latitude', 'longitude']])
        
        dbscan = DBSCAN(eps=0.002, min_samples=5, metric='euclidean')
        group['cluster'] = dbscan.fit_predict(group[['latitude', 'longitude']])

        # Inclui todos os pontos, agora com rótulos de cluster
        for index, row in group.iterrows():
            if int(row['cluster']) != -1:  # Filtrando para não incluir pontos classificados como ruído
                results.append((name, row['latitude'], row['longitude'], int(row['cluster'])))

    return results

# Define o esquema para o novo DataFrame do Spark
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("cluster", IntegerType(), True)
])

# Converte DataFrame em RDD e aplica a função a cada partição
clustered_rdd = df.rdd.mapPartitions(apply_dbscan)

# Converte o RDD de volta para DataFrame
final_df = spark.createDataFrame(clustered_rdd, schema)

# Converte o DataFrame do Spark para um DataFrame do Pandas
pandas_df = final_df.toPandas()

from sklearn.metrics import silhouette_score, calinski_harabasz_score, davies_bouldin_score

# Extrair os rótulos dos clusters do DataFrame pandas_df
labels = pandas_df['cluster']

# Calcular silhouette_score
silhouette = silhouette_score(pandas_df[['latitude', 'longitude']], labels)

# Calcular calinski_harabasz_score
calinski_harabasz = calinski_harabasz_score(pandas_df[['latitude', 'longitude']], labels)

# Calcular davies_bouldin_score
davies_bouldin = davies_bouldin_score(pandas_df[['latitude', 'longitude']], labels)

print("Silhouette Score:", silhouette)
print("Calinski Harabasz Score:", calinski_harabasz)
print("Davies Bouldin Score:", davies_bouldin)


# Print de Optimal number of clusters

import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import silhouette_score, calinski_harabasz_score, davies_bouldin_score

# Listas para armazenar os resultados das métricas
silhouette_scores = []
calinski_harabasz_scores = []
davies_bouldin_scores = []

# Variando o número de clusters
num_clusters = range(2, 11)  # Vamos variar de 2 a 10 clusters

# Realiza a clusterização com o número de clusters atual
dbscan = DBSCAN(eps=0.001, min_samples=5, metric='euclidean', n_jobs=-1)
labels = dbscan.fit_predict(pandas_df[['latitude', 'longitude']])

# Loop sobre o número de clusters e calculando as métricas para cada número de clusters
for n_clusters in num_clusters:
    # Extrai os rótulos de clusters únicos correspondentes ao número atual de clusters
    unique_labels = np.unique(labels)
    unique_labels = unique_labels[unique_labels != -1]  # Remove o rótulo -1 (rótulo de ruído)
    unique_labels = unique_labels[:n_clusters]  # Seleciona os primeiros n_clusters rótulos
    
    # Filtra os pontos pertencentes aos clusters selecionados
    filtered_points = pandas_df[labels != -1]  # Remove os pontos classificados como ruído
    filtered_points = filtered_points[np.isin(labels, unique_labels)]
    
    # Calcula as métricas de avaliação para os clusters selecionados
    silhouette = silhouette_score(filtered_points[['latitude', 'longitude']], filtered_points['cluster'])
    calinski_harabasz = calinski_harabasz_score(filtered_points[['latitude', 'longitude']], filtered_points['cluster'])
    davies_bouldin = davies_bouldin_score(filtered_points[['latitude', 'longitude']], filtered_points['cluster'])
    
    # Armazena os resultados das métricas
    silhouette_scores.append(silhouette)
    calinski_harabasz_scores.append(calinski_harabasz)
    davies_bouldin_scores.append(davies_bouldin)

# Imprimir os dados das métricas de avaliação
print("Número de Clusters | Silhouette Score | Calinski Harabasz Score | Davies Bouldin Score")
for i in range(len(num_clusters)):
    print(f"{num_clusters[i]:<18} | {silhouette_scores[i]:<16.6f} | {calinski_harabasz_scores[i]:<23.6f} | {davies_bouldin_scores[i]:<18.6f}")

# Plotando os gráficos
plt.figure(figsize=(12, 6))

# Silhouette Score
plt.subplot(1, 3, 1)
plt.plot(num_clusters, silhouette_scores, marker='o')
plt.title('Silhouette Score')
plt.xlabel('Número de Clusters')
plt.ylabel('Score')
plt.grid(True)

# Calinski Harabasz Score
plt.subplot(1, 3, 2)
plt.plot(num_clusters, calinski_harabasz_scores, marker='o')
plt.title('Calinski Harabasz Score')
plt.xlabel('Número de Clusters')
plt.ylabel('Score')
plt.grid(True)

# Davies Bouldin Score
plt.subplot(1, 3, 3)
plt.plot(num_clusters, davies_bouldin_scores, marker='o')
plt.title('Davies Bouldin Score')
plt.xlabel('Número de Clusters')
plt.ylabel('Score')
plt.grid(True)

plt.tight_layout()
plt.show()
