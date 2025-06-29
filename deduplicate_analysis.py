from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

MONGO_URI = os.getenv(
    "MONGO_URI", "mongodb+srv://usuario:password@cluster0...")
DB_NAME = "imdb_scrapper"
COL_NAME = "top250_movies"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COL_NAME]

pipeline = [
    {"$group": {"_id": {"title": "$title", "year": "$year"}, "count": {"$sum": 1}}},
    {"$match": {"count": {"$gt": 1}}},
    {"$sort": {"count": -1}}
]

duplicates = list(collection.aggregate(pipeline))
total_docs = collection.count_documents({})
total_dup_groups = len(duplicates)
total_dup_docs = sum(item["count"] - 1 for item in duplicates)
unique_movies = total_docs - total_dup_docs

print(f"Total documentos:           {total_docs}")
print(f"Películas únicas:           {unique_movies}")
print(f"Grupos con duplicados:      {total_dup_groups}")
print(f"Documentos duplicados:      {total_dup_docs}\n")

if duplicates:
    print("Top grupos duplicados (hasta 10):")
    for item in duplicates[:10]:
        t = item["_id"]["title"]
        y = item["_id"]["year"]
        c = item["count"]
        print(f"  • {t} ({y}) → {c} copias")
else:
    print("🎉 No se encontraron duplicados.")
