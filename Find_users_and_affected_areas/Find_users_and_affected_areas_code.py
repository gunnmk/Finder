# Databricks notebook source
# Define the path to the file
file_path = "dbfs:/run/Finder/union/df_union_standardized"

# Read the Parquet file into a DataFrame
df_union_standardized = (spark.read
      .format("parquet")
      .load(file_path)
     )

# Display the DataFrame
display(df_union_standardized)

# COMMAND ----------

from pyspark.sql.functions import udf, col, when, lit
from pyspark.sql.types import BooleanType, DoubleType
from shapely import wkt
from shapely.geometry import Point, Polygon

# COMMAND ----------

from pyspark.sql import Row

# Criando os dados de exemplo 
data = [ 
    Row(id_customer=1, latitude=-21.09146171090069, longitude=-47.910968363684646, polig_customer=None, description="Cliente 1 - Latitude e Longitude"),
    Row(id_customer=2, latitude=None, longitude=None, polig_customer="POLYGON ((-52.210130179415636 -8.421386265041527, -52.21081230822604 -8.421590012177138, -52.21152124941119 -8.421657333063996, -52.21222975800284 -8.42158564063317, -52.212910605700465 -8.421377690162105, -52.21353762734812 -8.421041473363951, -52.21408672653214 -8.420589911225743, -52.214536801642105 -8.420040357406652, -52.214870556799326 -8.41941393128878, -52.21507516648894 -8.418734706319873, -52.215142768359634 -8.41802878484708, -52.215070765265224 -8.417323295001356, -52.21486192495638 -8.416645348184055, -52.2145242736092 -8.416020997218062, -52.21407078729849 -8.415474235197145, -52.213518893285446 -8.415026073500098, -52.212889800292245 -8.414693734392985, -52.21220768350165 -8.414489989238294, -52.21149875559571 -8.41442266773559, -52.21126199317877 -8.414446624586416, -52.21126274013816 -8.414438825438735, -52.21122997616581 -8.414117763579434, -52.21134972668356 -8.414105646649963, -52.21203056256648 -8.413897699412688, -52.21265757405846 -8.413561485493487, -52.21320666513642 -8.413109925768426, -52.213656734501484 -8.41256037380362, -52.21399048649554 -8.411933948909596, -52.214195095724776 -8.411254724486842, -52.21426269985502 -8.410548802861399, -52.2141907016525 -8.409843312170114, -52.21398186867927 -8.40916536384697, -52.213644226829 -8.408541010772964, -52.21319075181121 -8.407994246123208, -52.212638870453084 -8.407546081377848, -52.212009790990685 -8.407213738920403, -52.21132768808727 -8.407009990242337, -52.210618773891824 -8.406942665178732, -52.20991029082472 -8.407014351029122, -52.209229464781714 -8.407222293123747, -52.20860245896613 -8.407558500656895, -52.208384039287196 -8.407738121482263, -52.20778772147893 -8.407559991919657, -52.207078805970646 -8.407492660533222, -52.20637032035877 -8.40756434023921, -52.20568949063673 -8.407772276603701, -52.20506248014902 -8.40810847904094, -52.204855619574516 -8.408278590749951, -52.20425775477373 -8.408099993607289, -52.2035488379746 -8.408032655913123, -52.20284034984282 -8.408104329488852, -52.202159516468534 -8.408312260135812, -52.20153250133635 -8.408648457487743, -52.20098339996261 -8.40910000203174, -52.200533313967355 -8.409649541542684, -52.200199540156824 -8.41027595786201, -52.199994905778155 -8.410955178405356, -52.199927275498574 -8.411661101221542, -52.19999924906661 -8.412366598058522, -52.200208061290404 -8.413044556892086, -52.20054568819403 -8.41366892385252, -52.200671032142516 -8.413820060829476, -52.20052487402769 -8.414305187769553, -52.200457244998645 -8.415011110506434, -52.20052922098945 -8.415716606907583, -52.20073803671543 -8.416394564965456, -52.201075668066295 -8.417018930840031, -52.201529140382775 -8.417565710137637, -52.20208102701599 -8.418013890057466, -52.202710119018064 -8.418346246959347, -52.203098711965595 -8.418462326363766, -52.20306721326674 -8.41879113958522, -52.203139195584626 -8.419496634613655, -52.20334801861489 -8.420174590234138, -52.2036856579667 -8.420798952700032, -52.2041391386733 -8.421345727748397, -52.20469103376323 -8.421793902741507, -52.20532013396328 -8.422126254228411, -52.206002262797625 -8.422330009883954, -52.20671120575364 -8.422397339379177, -52.207419717795005 -8.422325655313909, -52.20772259859377 -8.422233150954815, -52.2083897258706 -8.42216565235995, -52.20907057753894 -8.421957708118908, -52.20969760424004 -8.421621496860025, -52.21004096420472 -8.421339134228674, -52.210130179415636 -8.421386265041527))", description="Cliente 2 - Polígono"),
    Row(id_customer=3, latitude=-29.98306324550126, longitude=-50.13338360729129, polig_customer=None, description="Cliente 3 - Latitude e Longitude"),
    Row(id_customer=4, latitude=None, longitude=None, polig_customer="""POLYGON ((-50.7543806969873 -29.97885065655, -50.7124765351197 -29.9643894100778, -50.7326896274661 -29.94943784427, -50.7638773699881 -29.9631426917744, -50.7543806969873 -29.97885065655))""", description="Cliente 4 - Latitude e Longitude")
]

# Criação do DataFrame
customer_location = spark.createDataFrame(data)

# Exibir o DataFrame
display(customer_location)


# COMMAND ----------

# Importações necessárias
from pyspark.sql.functions import udf, col, when
from shapely.geometry import Point
from pyspark.sql.types import StringType

# UDF para converter latitude e longitude em WKT de ponto
def lat_long_to_wkt(lat, lon):
    if lat is not None and lon is not None:
        point = Point(lon, lat)
        return point.wkt  # Retorna o WKT do ponto
    else:
        return None

udf_lat_long_to_wkt = udf(lat_long_to_wkt, StringType())

# Converter polig_customer em WKT (já está em WKT se não for nulo)
customer_location = customer_location.withColumn(
    "geom_customer",
    when(
        col("polig_customer").isNotNull(),
        col("polig_customer")  # Mantém o WKT do polígono
    ).otherwise(
        udf_lat_long_to_wkt(col("latitude"), col("longitude"))  # Converte lat/lon em WKT de ponto
    )
)

# Converter polig do evento em WKT (já está em WKT)
df_union_standardized = df_union_standardized.withColumn(
    "geom_event",
    col("polig")  # Mantém o WKT do polígono do evento
)

# Exibir os DataFrames
customer_location.show(truncate=False)
df_union_standardized.show(truncate=False)


# COMMAND ----------

display(customer_location)

# COMMAND ----------

display(df_union_standardized)

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
import shapely.wkt

def is_in_event(wkt_geom_customer, wkt_geom_event):
    if wkt_geom_customer and wkt_geom_event:
        geom_customer = shapely.wkt.loads(wkt_geom_customer)
        geom_event = shapely.wkt.loads(wkt_geom_event)
        return geom_customer.intersects(geom_event)
    else:
        return False

udf_is_in_event = udf(is_in_event, BooleanType())


# COMMAND ----------

import math
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import shapely.wkt
from shapely.geometry import Point
from shapely.ops import nearest_points

def haversine(lat1, lon1, lat2, lon2):
    import math

    # Converter graus decimais para radianos
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Fórmula de Haversine
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    R = 6371.0  # Raio da Terra em quilômetros
    distance = R * c
    return distance



from pyspark.sql.types import DoubleType

def distance_to_event(wkt_geom_customer, wkt_geom_event):
    import math
    from shapely.geometry import Point
    from shapely.ops import nearest_points

    if wkt_geom_customer and wkt_geom_event:
        geom_customer = shapely.wkt.loads(wkt_geom_customer)
        geom_event = shapely.wkt.loads(wkt_geom_event)

        # Verificar se as geometrias são válidas
        if not geom_customer.is_valid or not geom_event.is_valid:
            return None

        # Encontrar os pontos mais próximos entre as geometrias do cliente e do evento
        nearest_points_pair = nearest_points(geom_customer, geom_event)
        point1 = nearest_points_pair[0]  # Ponto mais próximo na geometria do cliente
        point2 = nearest_points_pair[1]  # Ponto mais próximo na geometria do evento

        # Extrair as coordenadas (latitude e longitude)
        lat1, lon1 = point1.y, point1.x
        lat2, lon2 = point2.y, point2.x

        # Calcular a distância usando a fórmula de Haversine
        distance = haversine(lat1, lon1, lat2, lon2)
        return distance
    else:
        return None


# Definir a UDF com o tipo de retorno DoubleType
udf_distance_to_event = udf(distance_to_event, DoubleType())



# COMMAND ----------

# Importações necessárias
from pyspark.sql.functions import udf, col, when
from pyspark.sql.types import DoubleType
import shapely.wkt
from shapely.geometry import shape
from shapely.ops import transform
from pyproj import Transformer

# Definir a função para calcular a porcentagem da área afetada com projeção
def percentage_area_affected(wkt_geom_customer, wkt_geom_event):
    if wkt_geom_customer and wkt_geom_event:
        # Carregar as geometrias a partir das strings WKT
        geom_customer = shapely.wkt.loads(wkt_geom_customer)
        geom_event = shapely.wkt.loads(wkt_geom_event)
        
        # Verificar se as geometrias são válidas
        if geom_customer.is_valid and geom_event.is_valid:
            # Definir o transformador para projetar as geometrias (EPSG:4326 para EPSG:32722 - UTM Zone 22S)
            # Ajuste o EPSG conforme a localização dos seus dados
            transformer = Transformer.from_crs('epsg:4326', 'epsg:32722', always_xy=True)
            
            # Projetar as geometrias
            projected_geom_customer = transform(transformer.transform, geom_customer)
            projected_geom_event = transform(transformer.transform, geom_event)
            
            # Verificar novamente se as geometrias projetadas são válidas
            if projected_geom_customer.is_valid and projected_geom_event.is_valid:
                # Verificar se há interseção entre as geometrias projetadas
                if projected_geom_customer.intersects(projected_geom_event):
                    # Calcular a interseção
                    intersection = projected_geom_customer.intersection(projected_geom_event)
                    
                    # Calcular as áreas (em metros quadrados)
                    intersection_area = intersection.area
                    customer_area = projected_geom_customer.area
                    
                    # Calcular a porcentagem da área afetada
                    if customer_area > 0:
                        return (intersection_area / customer_area) * 100
                    else:
                        return 0.0
                else:
                    return 0.0  # Sem interseção
            else:
                return 0.0  # Geometrias projetadas inválidas
        else:
            return 0.0  # Geometrias originais inválidas
    else:
        return 0.0  # Geometrias nulas

# Registrar a UDF no PySpark
udf_percentage_area_affected = udf(percentage_area_affected, DoubleType())


# COMMAND ----------

crossed_df = customer_location.crossJoin(df_union_standardized)
display(crossed_df)

# COMMAND ----------

crossed_df = crossed_df.withColumn(
    "is_in_event",
    udf_is_in_event(col("geom_customer"), col("geom_event"))
)

# COMMAND ----------

from pyspark.sql.functions import when

crossed_df = crossed_df.withColumn(
    "distance_to_event",
    when(
        col("is_in_event") == False,
        udf_distance_to_event(col("geom_customer"), col("geom_event"))
    ).otherwise(None)
)


# COMMAND ----------

crossed_df = crossed_df.withColumn(
    "percentage_area_affected",
    when(
        col("polig_customer").isNotNull(),
        udf_percentage_area_affected(col("geom_customer"), col("geom_event"))
    ).otherwise(None)
)


# COMMAND ----------

result_df = crossed_df.select(
    "id_customer",
    "latitude",
    "longitude",
    "polig_customer",
    "description",
    "tipo_evento",
    "origem",
    "polig",
    "additional_value",
    "data_run",
    "id_polig_evento",
    "is_in_event",
    "distance_to_event",
    "percentage_area_affected"
)
display(result_df)

# COMMAND ----------

result_df_test = result_df.filter((col("id_customer") == 4) & (col("percentage_area_affected") != 0))
display(result_df_test)

# COMMAND ----------

result_df_filter = result_df.filter(col("is_in_event") == True)
display(result_df_filter)