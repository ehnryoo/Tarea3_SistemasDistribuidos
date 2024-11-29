# Tarea 3 | Sistemas Distribuidos | 2-2024
En este repositorio se encuentran todos los códigos implementados para  levantar el sistema de Kafka solicitado para la tarea.

Integrantes:
* Cristóbal Barra
* Jorge Gallegos

## Stack de tecnologías usado

[![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white&style=flat)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white&style=flat)](https://www.python.org/)
[![Static Badge](https://img.shields.io/badge/Apache%20Kafka-black?logo=apachekafka)](https://kafka.apache.org/documentation/)


## Instrucciones de uso

En la terminal utilizar los siguientes comandos:

```bash
 git clone https://github.com/ehnryoo/Tarea2_SistemasDistribuidos
```
Desde la carpeta T1_Sistema_Distribuidos, levantar los contenedores y arrancar la API
```bash
docker-compose up --build
```
Para iniciar el servidor gRPC y generar compras
```bash
python server.py
```
```bash
python generar_compra.py
```