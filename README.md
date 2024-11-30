# Tarea 3 | Sistemas Distribuidos | 2-2024
En este repositorio se encuentran todos los códigos implementados para  levantar el sistema de Kafka solicitado para la tarea.

Integrantes:
* Cristóbal Barra
* Jorge Gallegos

## Stack de tecnologías usado

[![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white&style=flat)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white&style=flat)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-black?logo=apachekafka)](https://kafka.apache.org/documentation/)


## Instrucciones de uso

En la terminal utilizar los siguientes comandos:

```bash
 git clone https://github.com/ehnryoo/Tarea3_SistemasDistribuidos
```
Desde la carpeta Tarea3_SistemasDistribuidos, levantar los contenedores.
```bash
docker-compose up --build
```
Para iniciar el scraper, dirigerse a la carpeta waze_scraper y ejecutar el siguiente comando.
```bash
scrapy crawl waze
```