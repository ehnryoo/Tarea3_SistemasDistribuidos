# Tarea 3 | Sistemas Distribuidos | 2-2024
En este repositorio se encuentran todos los códigos implementados para  levantar el sistema de Kafka solicitado para la tarea.

Integrantes:
* Cristóbal Barra
* Jorge Gallegos

## Stack de tecnologías usado

[![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white&style=flat)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white&style=flat)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-black?logo=apachekafka)](https://kafka.apache.org/documentation/)
[![Cassandra](https://img.shields.io/badge/Cassandra-1287B1?logo=apache-cassandra&logoColor=white&style=flat)](https://cassandra.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?logo=apache-spark&logoColor=white&style=flat)](https://spark.apache.org/)
[![Elasticsearch](https://img.shields.io/badge/Elasticsearch-005571?logo=elasticsearch&logoColor=white&style=flat)](https://www.elastic.co/elasticsearch/)



## Instrucciones de uso

En la terminal utilizar los siguientes comandos:
```bash
 git clone https://github.com/ehnryoo/Tarea3_SistemasDistribuidos
```

Desde la carpeta Tarea3_SistemasDistribuidos, levantar los contenedores.
```bash
docker-compose up -d
```

Primero iniciar los tópicos para el Kafka, dirigirse a la carpeta /kafka.
```bash
python createTopic.py
```

Para iniciar el scraper, dirigerse a la carpeta /waze_scraper y ejecutar el siguiente comando.
```bash
scrapy crawl waze
```

Luego de enviados los mensajes a los tópicos, inicializar Spark dentro de la carpeta /spark.
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.17.5,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 spark_process.py
```

Ya con los datos consumidos y procesados, es posible acceder a Kibana para analizar los incidents y jams. A través de la URL localhost:5061
