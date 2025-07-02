import os
import time
import html
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from pymongo import MongoClient
from dotenv import load_dotenv

# --- 1) Se obtienen las variables necesarias para la configuración de conexión con MongoDB ---
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB",   "movie-analysis-2025")
MONGO_COLL = os.getenv("MONGO_COLL", "imdb-scraper")
if not MONGO_URI:
    raise RuntimeError("Define MONGO_URI en tu .env")

client = MongoClient(MONGO_URI)
collection = client[MONGO_DB][MONGO_COLL]

# --- 2) Configuración Selenium ---
opts = Options()
# opts.add_argument("--headless")  # descomentar esta linea de codigo para NO ver el navegador
opts.add_argument("--disable-gpu")
opts.add_argument("--no-sandbox")
opts.add_experimental_option("prefs", {"intl.accept_languages": "es-ES"})
service = Service(ChromeDriverManager().install())

# --- 3) Se indican las URL y reglas de formato para extraer los datos ---
LIST_URLS = [
    "https://www.imdb.com/es-es/list/ls050782187/?view=detailed&count=250&page=1",
    "https://www.imdb.com/es-es/list/ls050782187/?view=detailed&count=250&page=2",
]


def parse_item(item):
    """Extrae todos los campos de un <li> de la lista detailed."""
    def safe_text(sel):
        try:
            return item.find_element(By.CSS_SELECTOR, sel).text.strip()
        except:
            return None

    # rank + title
    header = safe_text("h3.ipc-title__text") or ""
    rank = None
    title = None
    if "." in header:
        parts = header.split(".", 1)
        try:
            rank = int(parts[0])
        except:
            rank = None
        title = parts[1].strip()

    # metadata: year, duration, age
    metas = item.find_elements(By.CSS_SELECTOR, "span.dli-title-metadata-item")
    year = int(metas[0].text) if len(
        metas) >= 1 and metas[0].text.isdigit() else None
    duration = metas[1].text if len(metas) >= 2 else None
    age = metas[2].text if len(metas) >= 3 else None

    # rating & votes
    try:
        rtext = item.find_element(
            By.CSS_SELECTOR, "span.ipc-rating-star--rating").text
        vtext = item.find_element(
            By.CSS_SELECTOR, "span.ipc-rating-star--voteCount").text
        rating = float(rtext.replace(",", "."))
        votes = vtext.strip("()").replace("\u00A0", "")
    except:
        rating = None
        votes = None

    # poster URL
    try:
        poster = item.find_element(
            By.CSS_SELECTOR, "img.ipc-image").get_attribute("src")
    except:
        poster = None

    # description
    desc = safe_text(
        ".title-description-plot-container .ipc-html-content-inner-div")

    # credits: director + cast
    dirs = []
    cast = []
    try:
        credits = item.find_elements(
            By.CSS_SELECTOR, ".title-description-credit a.ipc-link")
        if credits:
            dirs = [credits[0].text]
            cast = [a.text for a in credits[1:]]
    except:
        pass

    return {
        "rank":          rank,
        "title":         html.unescape(title or ""),
        "year":          year,
        "duration":      duration,
        "age_rating":    age,
        "rating_value":  rating,
        "rating_count":  votes,
        "poster_url":    poster,
        "description":   html.unescape(desc or ""),
        "directors":     dirs,
        "cast":          cast,
    }


def main():
    driver = webdriver.Chrome(service=service, options=opts)
    wait = WebDriverWait(driver, 15)

    all_movies = []
    for page_idx, url in enumerate(LIST_URLS):
        print(f"\n🌐 Abriendo página {page_idx+1}: {url}")
        driver.get(url)
        # esperamos a que cargue al menos un ítem
        wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "li.ipc-metadata-list-summary-item")))
        time.sleep(1)  # pequeña pausa

        items = driver.find_elements(By.CSS_SELECTOR,
                                     "li.ipc-metadata-list-summary-item")
        print(f"   → Ítems detectados: {len(items)}")

        # parseamos sólo los primeros 250
        offset = page_idx * 250
        for idx, li in enumerate(items[:250], start=1):
            movie = parse_item(li)
            # si rank venía None, lo rellenamos por orden
            if movie["rank"] is None:
                movie["rank"] = offset + idx
            all_movies.append(movie)

    driver.quit()

    # elimina duplicados por (title, year), conservando el primero
    seen = set()
    unique = []
    for m in all_movies:
        key = (m["title"], m["year"])
        if key not in seen:
            seen.add(key)
            unique.append(m)
    movies = unique[:500]
    print(f"\n🔢 Total únicos preparados: {len(movies)}\n")

    # inserta sólo nuevos registros de peliculas
    for i, m in enumerate(movies, start=1):
        if collection.find_one({"title": m["title"], "year": m["year"]}):
            print(f"{i}/{len(movies)} → '{m['title']}' ya existe, omitiendo")
            continue
        try:
            collection.insert_one(m)
            print(
                f"{i}/{len(movies)} → ✅ Guardado: {m['title']} ({m['year']}) rank={m['rank']}")
        except Exception as e:
            print(f"{i}/{len(movies)} → ❌ Error insert: {e}")

    print(f"\n🎉 Completado! Total en DB: {collection.count_documents({})}")
    client.close()


if __name__ == "__main__":
    main()
