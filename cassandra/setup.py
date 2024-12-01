from cassandra.io.asyncio import AsyncioConnection
from cassandra.cluster import Cluster

def setup_cassandra():
    cluster = Cluster(['localhost'], connection_class=AsyncioConnection)
    session = cluster.connect()

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS waze
        WITH REPLICATION = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS waze.incidents (
            id TEXT PRIMARY KEY,
            reportBy TEXT,
            nThumbsUp INT,
            country TEXT,
            city TEXT,
            type TEXT,
            subtype TEXT,
            street TEXT,
            reportRating INT,
            reliability INT,
            longitude DOUBLE,
            latitude DOUBLE,
            timestamp BIGINT,
            additional_info TEXT
        )
    """)

    print("Keyspace y tabla creados o existentes listos para usar.")

if __name__ == "__main__":
    setup_cassandra()

