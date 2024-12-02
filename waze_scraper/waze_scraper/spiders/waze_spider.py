import scrapy
import json
from confluent_kafka import Producer

class WazeSpider(scrapy.Spider):
    name = 'waze'
    start_urls = ['https://www.waze.com/live-map/api/georss?top=-33.169490403827595&bottom=-33.75172616844491&left=-70.7432327270508&right=-70.46060943603517&env=row&types=alerts,traffic']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kafka_producer = self.create_kafka_producer()
        self.kafka_topic = 'incidente'

    def create_kafka_producer(self):
        return Producer({'bootstrap.servers': 'localhost:9093'})

    def delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(f"Mensaje fallido: {err}")
        else:
            self.logger.info(f"Mensaje enviado: {msg.key().decode('utf-8')} a {msg.topic()}")

    def parse(self, response):
        try:
            incidents = response.json().get("alerts", [])
        except json.JSONDecodeError:
            self.logger.error("Error al decodificar la respuesta JSON")
            return

        for incident in incidents:
            data = {
                "reportBy": incident.get("reportBy"),
                "nThumbsUp": incident.get("nThumbsUp"),
                "country": incident.get("country"),
                "city": incident.get("city"),
                "type": incident.get("type"),
                "subtype": incident.get("subtype"),
                "street": incident.get("street"),
                "reportRating": incident.get("reportRating"),
                "reliability": incident.get("reliability"),
                "location": {
                    "longitude": incident["location"].get("x"),
                    "latitude": incident["location"].get("y")
                },
                "timestamp": incident.get("pubMillis"),
                "id": incident.get("id"),
                "additional_info": incident.get("additionalInfo")
            }

            message = json.dumps(data)
            self.kafka_producer.produce(
                self.kafka_topic,
                key=data['id'],
                value=message,
                callback=self.delivery_report
            )

        self.kafka_producer.flush()

    def close(self, reason):
        self.kafka_producer.flush()
        self.logger.info("Kafka Producer cerrado.")

# COMANDO:
# scrapy crawl waze

# COMANDO PARA LOPP:
# while ($true) {scrapy crawl waze;  Start-Sleep -Seconds 15}
