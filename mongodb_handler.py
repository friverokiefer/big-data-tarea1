from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Cargar variables de entorno desde .env
dotenv_loaded = load_dotenv()

# Obtener URI de MongoDB (obligatorio)
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError(
        "❌ No se encontró la variable MONGO_URI en el entorno. "
        "Por favor, define MONGO_URI en tu archivo .env"
    )

# Parámetros de conexión
db_name = "imdb_scrapper"
collection_name = "top250_movies"

# Conexión a MongoDB con validación mínima
test_timeout = 5000
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=test_timeout)
try:
    client.server_info()  # Forzar conexión
except Exception as e:
    raise RuntimeError(f"❌ No se pudo conectar a MongoDB: {e}")

db = client[db_name]
collection = db[collection_name]


def insert_movie(movie_data: dict):
    """
    Inserta un documento de película en MongoDB.
    """
    result = collection.insert_one(movie_data)
    print(f"✅ Película insertada con _id: {result.inserted_id}")
