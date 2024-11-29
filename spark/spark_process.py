from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

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

spark = SparkSession.builder \
    .appName("WazeTrafficMonitoring") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
    .getOrCreate()

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:9092") \
    .option("subscribe", "incidente") \
    .load()

parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

filtered_df = parsed_df.filter(col("reportRating") >= 3)

cassandra_writer = filtered_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "waze") \
    .option("table", "incidents") \
    .outputMode("append") \
    .start()

elasticsearch_writer = filtered_df.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "localhost") \
    .option("es.resource", "waze/incident") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()

