import scrapy
import json
from confluent_kafka import Producer

# Ciudades
CITIES = {
    'santiago': 'https://www.waze.com/live-map/api/georss?top=-33.17868612693965&bottom=-33.760860332046704&left=-70.77687835693361&right=-70.49425506591798&env=row&types=alerts,traffic',
    'los_angeles': 'https://www.waze.com/live-map/api/georss?top=34.48305134013095&bottom=33.32458739400334&left=-118.39045715332033&right=-117.82521057128906&env=na&types=alerts,traffic',
    'london': 'https://www.waze.com/live-map/api/georss?top=51.73939109053308&bottom=51.305149419672766&left=-0.20757293701171875&right=0.07505035400390625&env=row&types=alerts,traffic',
    'madrid': 'https://www.waze.com/live-map/api/georss?top=40.653559088834264&bottom=40.12198326924039&left=-3.838554382324219&right=-3.555931091308594&env=row&types=alerts,traffic',
    'istanbul': 'https://www.waze.com/live-map/api/georss?top=41.26026515060583&bottom=40.733526863769605&left=28.876670837402347&right=29.159294128417972&env=row&types=alerts,traffic',
    'berlin': 'https://www.waze.com/live-map/api/georss?top=52.62032722085264&bottom=52.4079666770112&left=13.34897232055664&right=13.490283966064455&env=row&types=alerts,traffic',
    'paris': 'https://www.waze.com/live-map/api/georss?top=48.91897870494946&bottom=48.80419401621832&left=2.3058757781982426&right=2.376531600952149&env=row&types=alerts,traffic',
    'buenos_aires': 'https://www.waze.com/live-map/api/georss?top=-34.47243922687763&bottom=-34.75961948083355&left=-58.509166717529304&right=-58.36785507202149&env=row&types=alerts,traffic',
    'mexico_city': 'https://www.waze.com/live-map/api/georss?top=19.600799237263313&bottom=19.27173245166788&left=-99.185359954834&right=-99.04404830932617&env=row&types=alerts,traffic',
    'sao_paulo': 'https://www.waze.com/live-map/api/georss?top=-23.479204271318615&bottom=-23.63913783212136&left=-46.652978897094734&right=-46.58232307434083&env=row&types=alerts,traffic',
    'rome': 'https://www.waze.com/live-map/api/georss?top=41.96520590644729&bottom=41.83534154806385&left=12.453638076782228&right=12.524293899536135&env=row&types=alerts,traffic',
    'new_york': 'https://www.waze.com/live-map/api/georss?top=41.040525456056784&bottom=40.512028155910166&left=-74.1874465942383&right=-73.90482330322267&env=na&types=alerts,traffic',
    'minneapolis': 'https://www.waze.com/live-map/api/georss?top=45.51536889450566&bottom=44.52878586769469&left=-93.55311584472658&right=-92.98786926269531&env=na&types=alerts,traffic',
    'dubai': 'https://www.waze.com/live-map/api/georss?top=25.62041780457344&bottom=24.989481694008877&left=55.1751937866211&right=55.457817077636726&env=row&types=alerts,traffic',
    'bogota': 'https://www.waze.com/live-map/api/georss?top=4.741994305854454&bottom=4.568093262805677&left=-74.11296272277833&right=-74.04230690002443&env=row&types=alerts,traffic',
    'miami': 'https://www.waze.com/live-map/api/georss?top=26.56165190268291&bottom=25.306425582525804&left=-80.46839904785158&right=-79.90315246582031&env=na&types=alerts,traffic',
    'seattle': 'https://www.waze.com/live-map/api/georss?top=47.80902760112089&bottom=47.338193386106724&left=-122.40712738037111&right=-122.12450408935548&env=na&types=alerts,traffic',
    'vancouver': 'https://www.waze.com/live-map/api/georss?top=49.43243914950216&bottom=48.97645676109397&left=-123.18578338623048&right=-122.90316009521486&env=na&types=alerts,traffic',
    'toronto': 'https://www.waze.com/live-map/api/georss?top=44.25145514735848&bottom=43.24315030308887&left=-79.81196594238283&right=-79.24671936035158&env=na&types=alerts,traffic',
    'lima': 'https://www.waze.com/live-map/api/georss?top=-11.729157710726337&bottom=-12.41163142433218&left=-77.1153030395508&right=-76.83267974853517&env=row&types=alerts,traffic'
}

class WazeSpider(scrapy.Spider):
    name = 'waze'
    start_urls = list(CITIES.values())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kafka_producer = self.create_kafka_producer()
        self.alerts_topic = 'incidents'
        self.jams_topic = 'jams'

    def create_kafka_producer(self):
        return Producer({'bootstrap.servers': 'localhost:9093'})

    def delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(f"Mensaje fallido: {err}")
        else:
            self.logger.info(f"Mensaje enviado: {msg.key().decode('utf-8')} a {msg.topic()}")

    def parse(self, response):
        city_name = [name for name, url in CITIES.items() if url == response.url]
        city_name = city_name[0] if city_name else "unknown_city"
        self.logger.info(f"Procesando datos para la ciudad: {city_name}")

        try:
            data = response.json()
            alerts = data.get("alerts", [])
            jams = data.get("jams", [])
        except json.JSONDecodeError:
            self.logger.error(f"Error al decodificar la respuesta JSON para {city_name}")
            return

        # Procesar "alerts" (incidentes)
        for alert in alerts:
            alert_data = {
                "reportBy": alert.get("reportBy"),
                "nThumbsUp": alert.get("nThumbsUp"),
                "country": alert.get("country"),
                "city": alert.get("city"),
                "type": alert.get("type"),
                "subtype": alert.get("subtype"),
                "street": alert.get("street"),
                "reportRating": alert.get("reportRating"),
                "reliability": alert.get("reliability"),
                "location": {
                    "longitude": alert["location"].get("x"),
                    "latitude": alert["location"].get("y")
                },
                "timestamp": alert.get("pubMillis"),
                "id": alert.get("id"),
                "additional_info": alert.get("additionalInfo")
            }

            message = json.dumps(alert_data)
            self.kafka_producer.produce(
                self.alerts_topic,
                key=str(alert_data['id']),
                value=message,
                callback=self.delivery_report
            )

        # Procesar "jams" (atascos)
        for jam in jams:
            jam_data = {
                "severity": jam.get("severity"),
                "country": jam.get("country"),
                "city": jam.get("city"),
                "speedKMH": jam.get("speedKMH"),
                "length": jam.get("length"),
                "roadType": jam.get("roadType"),
                "delay": jam.get("delay"),
                "street": jam.get("street"),
                "id": jam.get("id"),
                "timestamp": jam.get("pubMillis"),
                "updateMillis": jam.get("updateMillis")
            }

            message = json.dumps(jam_data)
            self.kafka_producer.produce(
                self.jams_topic,
                key=str(jam_data['id']),
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
