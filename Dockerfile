# Usar la imagen base de Python
FROM python:3.9-slim

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar los archivos de requerimientos y el archivo de configuración de Cassandra
COPY requirements.txt .
COPY cassandra/setup.py cassandra/setup.py
COPY kafka/createTopic.py kafka/createTopic.py
COPY spark/spark_process.py spark/spark_process.py
COPY waze_scraper waze_scraper/
COPY scrapy.cfg scrapy.cfg

# Instalar las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Configurar e inicializar Cassandra
RUN python cassandra/setup.py

# Crear el tópico en Kafka
RUN python kafka/createTopic.py

# Ejecutar Spark y luego el scraper
CMD ["sh", "-c", "spark-submit spark/spark_process.py && cd waze_scraper && scrapy crawl waze"]
