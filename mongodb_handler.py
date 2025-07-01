# mongodb_handler.py

from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Carga variables de entorno de .env
load_dotenv()

# URI obligatoria
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("❌ Define MONGO_URI en tu .env")

# Base y colección dinámicas
MONGO_DB = os.getenv("MONGO_DB", "movie-analysis-2025")
MONGO_COLL = os.getenv("MONGO_COLL", "imdb-scraper")

# Conexión a Mongo
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLL]


def insert_movie(movie_data: dict):
    """
    Inserta un documento, imprimiendo éxito o error.
    """
    try:
        result = collection.insert_one(movie_data)
        print(
            f"✅ Insertada '{movie_data.get('title')}' ({movie_data.get('year')}) → _id: {result.inserted_id}")
    except Exception as e:
        print(f"❌ Error al insertar '{movie_data.get('title')}': {e}")
