from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Conexión a Cassandra
def connect_cassandra():
    # Aquí puedes configurar tus credenciales de Cassandra si es necesario
    cluster = Cluster(['localhost'], port=9042)  # Cambia 'localhost' si es necesario
    session = cluster.connect()
    return session

# Crear keyspace
def create_keyspace(session):
    keyspace_creation_query = """
    CREATE KEYSPACE IF NOT EXISTS my_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """
    session.execute(keyspace_creation_query)
    print("Keyspace 'my_keyspace' creado correctamente.")

# Crear tabla 'incidents'
def create_incidents_table(session):
    incidents_table_query = """
    CREATE TABLE IF NOT EXISTS my_keyspace.incidents (
        reportBy TEXT,
        nThumbsUp BIGINT,
        country TEXT,
        city TEXT,
        type TEXT,
        subtype TEXT,
        street TEXT,
        reportRating BIGINT,
        reliability BIGINT,
        location MAP<TEXT, DOUBLE>,
        timestamp BIGINT,
        id TEXT PRIMARY KEY,
        additional_info TEXT
    );
    """
    session.execute(incidents_table_query)
    print("Tabla 'incidents' creada correctamente.")

# Crear tabla 'jams'
def create_jams_table(session):
    jams_table_query = """
    CREATE TABLE IF NOT EXISTS my_keyspace.jams (
        severity BIGINT,
        country TEXT,
        city TEXT,
        speedKMH BIGINT,
        length BIGINT,
        roadType TEXT,
        delay BIGINT,
        street TEXT,
        id TEXT PRIMARY KEY,
        timestamp BIGINT,
        updateMillis BIGINT
    );
    """
    session.execute(jams_table_query)
    print("Tabla 'jams' creada correctamente.")

# Función principal
def main():
    session = connect_cassandra()
    
    # Usar el keyspace
    session.set_keyspace('my_keyspace')
    
    # Crear keyspace y tablas
    create_keyspace(session)
    create_incidents_table(session)
    create_jams_table(session)

    # Cerrar la conexión
    session.cluster.shutdown()
    print("Conexión cerrada.")

if __name__ == "__main__":
    main()
