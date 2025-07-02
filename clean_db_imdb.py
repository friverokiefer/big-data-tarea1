import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "movie-analysis-2025")
MONGO_COLL = os.getenv("MONGO_COLL", "imdb-scraper")

if not MONGO_URI:
    raise RuntimeError("Define MONGO_URI en tu .env")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLL]

confirm = input(
    f"⚠️ Vas a BORRAR todos los documentos de '{MONGO_DB}.{MONGO_COLL}'. ¿Confirmas? (s/n): "
).strip().lower()

if confirm == "s":
    result = collection.delete_many({})
    print(f"✅ Eliminados {result.deleted_count} documentos.")
else:
    print("❌ Operación cancelada.")

client.close()
