from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Esquema de los datos
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

# Crear la sesión de Spark con configuración para Kafka, Cassandra y Elasticsearch
spark = SparkSession.builder \
    .appName("WazeIncidentProcessor") \
    .config("spark.es.nodes", "localhost") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
    .config("spark.hadoop.io.nativeio", "false") \
    .getOrCreate()

# Configurar nivel de log
spark.sparkContext.setLogLevel("INFO")

# Leer datos de Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9093") \
    .option("subscribe", "incidente") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Convertir datos de Kafka a formato string
value_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Parsear JSON y seleccionar columnas relevantes
parsed_df = value_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Procesar y escribir los datos por batch
def process_batch(df, epoch_id):
    print(f"Procesando batch {epoch_id}")
    df.show(truncate=False)

    # Escribir a Elasticsearch
    try:
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "localhost") \
            .option("es.port", "9200") \
            .option("es.resource", "waze/incidente") \
            .mode("append") \
            .save()
        print(f"Batch {epoch_id} escrito en Elasticsearch exitosamente")
    except Exception as e:
        print(f"Error escribiendo en Elasticsearch: {str(e)}")

    # Escribir a Cassandra
    try:
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "your_keyspace") \
            .option("table", "incidente") \
            .mode("append") \
            .save()
        print(f"Batch {epoch_id} escrito en Cassandra exitosamente")
    except Exception as e:
        print(f"Error escribiendo en Cassandra: {str(e)}")

# Iniciar el streaming con foreachBatch
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()

# Esperar a que termine el streaming
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Deteniendo el streaming...")
    query.stop()
    print("Streaming detenido")
finally:
    spark.stop()


# COMANDO:
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.17.5,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 sp1.py  