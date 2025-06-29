import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from tqdm import tqdm

from mongodb_handler import insert_movie


def scrape_top_250_movies():
    options = Options()
    # Comentar la siguiente línea si quieres ver el navegador en acción:
    # options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_experimental_option(
        "prefs", {"intl.accept_languages": "en-US"}
    )

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )
    wait = WebDriverWait(driver, 20)

    print("🔍 Abriendo IMDb Top 250 en vista detallada…")
    driver.get("https://www.imdb.com/chart/top/?ref_=nv_mv_250")

    # Cambiar a vista detallada (lista con sinopsis y créditos)
    try:
        btn = wait.until(
            EC.element_to_be_clickable((By.ID, "list-view-option-detailed"))
        )
        btn.click()
        time.sleep(1)
    except Exception as e:
        print(f"⚠️ No se pudo cambiar a vista detallada: {e}")

    # Esperar a que carguen los elementos detallados
    wait.until(
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, "li.ipc-metadata-list-summary-item")
        )
    )
    items = driver.find_elements(
        By.CSS_SELECTOR, "li.ipc-metadata-list-summary-item")
    total = len(items)
    print(f"🔢 Encontrados {total} ítems en vista detallada.")

    for idx, item in enumerate(tqdm(items, desc="Scrapeando películas"), start=1):
        try:
            # Título y ranking
            header = item.find_element(By.CSS_SELECTOR, "h3.ipc-title__text")
            text = header.text.strip()
            rank = int(text.split('.')[0])
            title = text.split('. ', 1)[1]

            # Metadatos básicos: año y duración
            meta = item.find_elements(
                By.CSS_SELECTOR, "span.dli-title-metadata-item")
            year = int(meta[0].text)
            duration = meta[1].text  # Ej. "2h 32m"

            # Rating y votos
            rating_elem = item.find_element(
                By.CSS_SELECTOR,
                "span.ratingGroup--imdb-rating .ipc-rating-star--rating"
            )
            rating = float(rating_elem.text.replace(',', '.'))
            votes_text = item.find_element(
                By.CSS_SELECTOR,
                "span.ratingGroup--imdb-rating .ipc-rating-star--voteCount"
            ).text
            votes = votes_text.strip('()')

            # URL del póster
            poster_url = item.find_element(
                By.CSS_SELECTOR, "img.ipc-poster__poster-image"
            ).get_attribute("src")

            # Sinopsis inline
            desc_el = item.find_element(
                By.CSS_SELECTOR,
                ".title-description-plot-container .ipc-html-content-inner-div"
            )
            description = desc_el.text.strip()

            # Créditos: primer enlace director, resto reparto
            credits = item.find_elements(
                By.CSS_SELECTOR, ".title-description-credit a.ipc-link"
            )
            directors = []
            cast = []
            if credits:
                directors.append(credits[0].text)
                cast = [a.text for a in credits[1:]]

            # Construir documento
            movie = {
                "rank": rank,
                "title": title,
                "year": year,
                "duration": duration,
                "rating": rating,
                "votes": votes,
                "poster_url": poster_url,
                "description": description,
                "directors": directors,
                "cast": cast,
            }

            insert_movie(movie)
            print(f"{idx}/{total} → {title} ({year}) insertada.")

        except Exception as e:
            print(f"❌ Error al procesar ítem {idx}: {e}")
            continue

    driver.quit()


if __name__ == "__main__":
    scrape_top_250_movies()
