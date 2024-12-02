from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Esquema de los datos para 'incidents'
incident_schema = StructType([
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

# Esquema de los datos para 'jams'
jam_schema = StructType([
    StructField("severity", LongType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("speedKMH", LongType(), True),
    StructField("length", LongType(), True),
    StructField("roadType", StringType(), True),
    StructField("delay", LongType(), True),
    StructField("street", StringType(), True),
    StructField("id", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("updateMillis", LongType(), True)
])

# Crear la sesión de Spark con configuración para Kafka, Cassandra y Elasticsearch
spark = SparkSession.builder \
    .appName("WazeDataProcessor") \
    .config("spark.es.nodes", "localhost") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
    .config("spark.hadoop.io.nativeio", "false") \
    .getOrCreate()

# Configurar nivel de log
spark.sparkContext.setLogLevel("INFO")

# Leer datos de Kafka desde ambos topics
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9093") \
    .option("subscribe", "incidents,jams") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Convertir los datos de Kafka a formato string
value_df = kafka_df.selectExpr("CAST(value AS STRING)", "topic")

# Parsear los datos dependiendo del topic
incident_df = value_df.filter(col("topic") == "incidents").select(
    from_json(col("value"), incident_schema).alias("data")
).select("data.*")

jam_df = value_df.filter(col("topic") == "jams").select(
    from_json(col("value"), jam_schema).alias("data")
).select("data.*")

# Procesar y escribir los datos por batch para incidents
def process_incidents_batch(df, epoch_id):
    #print(f"Procesando incidents batch {epoch_id}")
    #df.show(truncate=False)

    # Escribir en Elasticsearch para incidents
    try:
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "localhost") \
            .option("es.port", "9200") \
            .option("es.resource", "waze-incidents") \
            .mode("append") \
            .save()
        print(f"Batch {epoch_id} escrito en Elasticsearch exitosamente")
    except Exception as e:
        print(f"Error escribiendo en Elasticsearch: {str(e)}")

    # Escribir en Cassandra para incidents
    try:
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "my_keyspace") \
            .option("table", "incidents") \
            .mode("append") \
            .save()
        print(f"Batch {epoch_id} escrito en Cassandra exitosamente")
    except Exception as e:
        print(f"Error escribiendo en Cassandra: {str(e)}")

# Procesar y escribir los datos por batch para jams
def process_jams_batch(df, epoch_id):
    #print(f"Procesando jams batch {epoch_id}")
    #df.show(truncate=False)

    # Escribir en Elasticsearch para jams
    try:
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "localhost") \
            .option("es.port", "9200") \
            .option("es.resource", "waze-jams") \
            .mode("append") \
            .save()
        print(f"Batch {epoch_id} escrito en Elasticsearch exitosamente")
    except Exception as e:
        print(f"Error escribiendo en Elasticsearch: {str(e)}")

    # Escribir en Cassandra para jams
    try:
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "my_keyspace") \
            .option("table", "jams") \
            .mode("append") \
            .save()
        print(f"Batch {epoch_id} escrito en Cassandra exitosamente")
    except Exception as e:
        print(f"Error escribiendo en Cassandra: {str(e)}")

# Iniciar el streaming para incidents
query_incidents = incident_df.writeStream \
    .foreachBatch(process_incidents_batch) \
    .outputMode("append") \
    .start()

# Iniciar el streaming para jams
query_jams = jam_df.writeStream \
    .foreachBatch(process_jams_batch) \
    .outputMode("append") \
    .start()

# Esperar a que termine el streaming
try:
    query_incidents.awaitTermination()
    query_jams.awaitTermination()
except KeyboardInterrupt:
    print("Deteniendo el streaming...")
    query_incidents.stop()
    query_jams.stop()
    print("Streaming detenido")
finally:
    spark.stop()


# COMANDO:
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.17.5,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 spark_process.py
