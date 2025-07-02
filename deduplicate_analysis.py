import os
from pymongo import MongoClient
from dotenv import load_dotenv


def get_collection():
    """
    Lee MONGO_URI, MONGO_DB y MONGO_COLL de .env y devuelve (client, collection).
    """
    load_dotenv()
    uri = os.getenv("MONGO_URI")
    if not uri:
        raise RuntimeError("❌ Define MONGO_URI en tu .env")
    db_name = os.getenv("MONGO_DB",   "movie-analysis-2025")
    coll_name = os.getenv("MONGO_COLL", "imdb-scraper")
    client = MongoClient(uri)
    return client, client[db_name][coll_name]


def analyze(collection):
    """
    Encuentra grupos con >1 documento según 'title' y 'year'.
    """
    pipeline = [
        {"$group": {
            "_id": {"title": "$title", "year": "$year"},
            "count": {"$sum": 1}
        }},
        {"$match": {"count": {"$gt": 1}}},
        {"$sort": {"count": -1}}
    ]
    dups = list(collection.aggregate(pipeline))
    total_docs = collection.count_documents({})
    dup_groups = len(dups)
    dup_docs = sum(g["count"] - 1 for g in dups)
    unique_est = total_docs - dup_docs

    print(f"\n📊 Estado de {collection.full_name}:")
    print(f"  • Total docs:           {total_docs}")
    print(f"  • Estimated uniques:    {unique_est}")
    print(f"  • Duplicate groups:     {dup_groups}")
    print(f"  • Duplicate docs:       {dup_docs}\n")

    if dup_groups:
        print("🔍 Top duplicate groups (title (year) → count):")
        for grp in dups[:10]:
            key = grp["_id"]
            title = key.get("title", "<no title>")
            year = key.get("year",  "<no year>")
            cnt = grp["count"]
            print(f"    – {title} ({year}) → {cnt}")
    else:
        print("🎉 No duplicates found.\n")

    return dups


def remove_duplicates(collection, duplicates):
    """
    Para cada par (title, year), deja el primer doc y borra el resto.
    """
    print("\n🗑 Removing duplicates...")
    for grp in duplicates:
        key = grp["_id"]
        title = key.get("title")
        year = key.get("year")
        if not title or not year:
            continue
        docs = list(
            collection
            .find({"title": title, "year": year}, {"_id": 1})
            .sort("_id", 1)
        )
        # keep first, delete the rest
        to_delete = [d["_id"] for d in docs[1:]]
        if to_delete:
            res = collection.delete_many({"_id": {"$in": to_delete}})
            print(
                f"  • Deleted {res.deleted_count} extra copy(ies) of '{title}' ({year})")
    print("✅ Duplicates removal complete.\n")


def main():
    client, coll = get_collection()
    try:
        duplicates = analyze(coll)
        if not duplicates:
            return
        ans = input(
            "Do you want to delete these duplicates? (y/n): ").strip().lower()
        if ans == 'y':
            remove_duplicates(coll, duplicates)
            print("🔄 Re-running analysis after removal:")
            analyze(coll)
        else:
            print("ℹ️ No changes made.")
    finally:
        client.close()
        print("👋 Connection closed.")


if __name__ == "__main__":
    main()
