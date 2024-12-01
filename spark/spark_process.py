from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Esquema de los datos en Kafka
schema = StructType([
    StructField("reportBy", StringType(), True),
    StructField("nThumbsUp", LongType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("type", StringType(), True),
    StructField("subtype", StringType(), True),
    StructField("street", StringType(), True),
    StructField("reportRating", LongType(), True),
    StructField("reliability", LongType(), True),
    StructField("location", StructType([
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True)
    ])),
    StructField("timestamp", LongType(), True),
    StructField("id", StringType(), True),
    StructField("additional_info", StringType(), True)
])

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("WazeTrafficMonitoring") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
    .getOrCreate()

# Leer datos del tópico Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9093") \
    .option("subscribe", "Incidente") \
    .load()

# Procesar datos de Kafka
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Filtrar datos
filtered_df = parsed_df.filter(col("reportRating") >= 3)

# Escribir datos filtrados en Cassandra
cassandra_writer = filtered_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "waze") \
    .option("table", "incidents") \
    .outputMode("append") \
    .start()

# Escribir datos filtrados en Elasticsearch
elasticsearch_writer = filtered_df.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "localhost") \
    .option("es.resource", "waze/incident") \
    .outputMode("append") \
    .start()

# Esperar a que finalicen los procesos
spark.streams.awaitAnyTermination()