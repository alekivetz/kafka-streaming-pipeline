import os
import psycopg2
import pytz
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.getenv("KAFKA_API_KEY"),
    "sasl.password": os.getenv("KAFKA_API_SECRET"),
}

SCHEMA_REGISTRY_CONFIG = {
    "url": os.getenv("SCHEMA_REGISTRY_URL"),
    "basic.auth.user.info": f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:{os.getenv('SCHEMA_REGISTRY_API_SECRET')}"
}

SCHEDULE = {
    range(11, 12): "quiet",
    range(12, 14): "busy",
    range(14, 15): "quiet",
    range(15, 16): "steady",
    range(16, 18): "quiet",
    range(18, 20): "busy",
    range(20, 22): "steady",
    range(22, 23): "quiet"
}

def load_schema(filename):
    """
    Loads the corresponding Avro schema for a given filename.
    Returns the contents of that file as a string.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base_dir, "..", "schemas", filename)
    with open(path, "r") as f:
        print(f"Loading schema for {filename}...\n")
        return f.read()
    
def get_current_profile():
    """
    Checks the current hour and returns the appropriate load profile name,
    or None if the restaurant is closed.
    """
    edmonton_tz = pytz.timezone("America/Edmonton")
    current_hour = datetime.now(edmonton_tz).hour
    for hours, profile in SCHEDULE.items():
        if current_hour in hours:
            return profile
    return None

def connect_to_db():
    """
    Establishes a connection to the Postgres database and returns a psycopg2 connection object.
    """
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
