from confluent_kafka.admin import AdminClient, NewTopic

def create_kafka_topic(topic_name, num_partitions, replication_factor):
    admin_client = AdminClient({
        'bootstrap.servers': 'localhost:9093'
    })

    topic_list = [NewTopic(topic_name, num_partitions, replication_factor)]
    futures = admin_client.create_topics(topic_list)

    for topic, future in futures.items():
        try:
            future.result()
            print(f"Tópico '{topic}' creado exitosamente")
        except Exception as e:
            print(f"Fallo al crear el tópico '{topic}': {e}")

if __name__ == "__main__":
    create_kafka_topic(topic_name="Incidente", num_partitions=1, replication_factor=1)

