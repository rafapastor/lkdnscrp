import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from datetime import datetime, timedelta
import re
import time
from langdetect import detect, DetectorFactory
DetectorFactory.seed = 0

# Configuración básica
location = "Spain"
csv_file = 'jobs.csv'
ultimo_dia = 'r86400'
tres_dias = 'r259200'
ultima_semana = 'r604800'
time_filter = ultimo_dia
max_jobs_to_scrap = 300
jobs_per_page = 10
max_pages = max_jobs_to_scrap // jobs_per_page

def main():
    scrape_jobs(
        title="developer", 
        location=location, 
        time_filter=time_filter, 
        csv_file=csv_file, 
        max_pages=max_pages, 
        jobs_per_page=jobs_per_page)
    scrape_jobs(
        title="programador", 
        location=location, 
        time_filter=time_filter, 
        csv_file=csv_file, 
        max_pages=max_pages, 
        jobs_per_page=jobs_per_page)
    scrape_jobs(
        title="python", 
        location=location, 
        time_filter=time_filter, 
        csv_file=csv_file, 
        max_pages=max_pages, 
        jobs_per_page=jobs_per_page)


# Eliminar archivo CSV si existe
if os.path.exists(csv_file):
    os.remove(csv_file)

# Guardar trabajos en el archivo CSV
def save_jobs_to_csv(jobs_df, csv_file):
    # Verifica si el archivo ya existe
    file_exists = os.path.isfile(csv_file)

    # Guarda el DataFrame en el archivo CSV, agregando (appending) si ya existe
    jobs_df.to_csv(csv_file, mode='a', header=not file_exists, index=False)
    print(f"{len(jobs_df)} trabajos guardados en {csv_file}.")


# Manejar solicitudes con reintentos en caso de error 429
def handle_request_with_retries(url, max_retries=5):
    wait_time = 10
    for attempt in range(max_retries):
        response = requests.get(url)
        if response.status_code == 429:
            time.sleep(wait_time)
        else:
            return response
    print(f"Error repetido 429 después de {max_retries} reintentos. Saltando {url}.")
    return None

# Extraer información de cada trabajo
def extract_job_details_and_match(base_card_div, job_soup, title):
    # Verificar que el base_card_div no sea None y que tenga el atributo 'data-entity-urn'
    if base_card_div is None or base_card_div.get("data-entity-urn") is None:
        print("Elemento base-card o data-entity-urn no encontrado, saltando este trabajo.")
        return None

    job_id = base_card_div.get("data-entity-urn").split(":")[3]
    job_description = job_soup.find("div", {"class": "description__text description__text--rich"})

    if job_description is None:
        print(f"Descripción del trabajo no encontrada para job_id {job_id}. Saltando este trabajo.")
        return None

    job_description_text = job_description.get_text()

    # Detectar el lenguaje de la descripción del trabajo
    language = detect(job_description_text)
    if language != 'es':
        return None

    # Si el título es "python", aplicar el nuevo filtro
    if title.lower() == "python":
        # Buscar la palabra "python" indiferentemente de mayúsculas o minúsculas
        if re.search(r'python', job_description_text, re.IGNORECASE):
            job_post = {
                'job_id': job_id,
                'job_title': job_soup.find("h2", {"class": "top-card-layout__title"}).text.strip() if job_soup.find("h2", {"class": "top-card-layout__title"}) else None,
                'company_name': job_soup.find("a", {"class": "topcard__org-name-link"}).text.strip() if job_soup.find("a", {"class": "topcard__org-name-link"}) else None,
                'time_posted': job_soup.find("span", {"class": "posted-time-ago__text"}).text.strip() if job_soup.find("span", {"class": "posted-time-ago__text"}) else None,
                'num_applicants': job_soup.find("span", {"class": "num-applicants__caption"}).text.strip() if job_soup.find("span", {"class": "num-applicants__caption"}) else None,
                'creation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            return job_post
        else:
            return None  # Si no encuentra la palabra "python", descartar el trabajo

    # Patrón para detectar números seguidos de "k"
    pattern_k = re.compile(r'\b(?!401k)\d+k\b', re.IGNORECASE)
    
    # Patrón para detectar "euros" y "brutos" en la misma línea
    pattern_euros_brutos = re.compile(r'\beuros\b.*\bbrutos\b|\bbrutos\b.*\beuros\b', re.IGNORECASE)

    # Aplicar los filtros pattern_k y pattern_euros_brutos para otros títulos
    if pattern_k.search(job_description_text) or pattern_euros_brutos.search(job_description_text):
        job_post = {
            'job_id': job_id,
            'job_title': job_soup.find("h2", {"class": "top-card-layout__title"}).text.strip() if job_soup.find("h2", {"class": "top-card-layout__title"}) else None,
            'company_name': job_soup.find("a", {"class": "topcard__org-name-link"}).text.strip() if job_soup.find("a", {"class": "topcard__org-name-link"}) else None,
            'time_posted': job_soup.find("span", {"class": "posted-time-ago__text"}).text.strip() if job_soup.find("span", {"class": "posted-time-ago__text"}) else None,
            'num_applicants': job_soup.find("span", {"class": "num-applicants__caption"}).text.strip() if job_soup.find("span", {"class": "num-applicants__caption"}) else None,
            'creation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        return job_post
    return None

def scrape_jobs(title, location, time_filter, csv_file, max_pages, jobs_per_page):
    all_jobs = []
    
    for page in range(max_pages):
        print(f"Scrapeando página {page + 1} para el título: {title}")
        jobs_on_page = scrape_jobs_from_page(page, title, location, time_filter, jobs_per_page)
        print(f"Encontrados {len(jobs_on_page)} trabajos en la página {page + 1}.")

        if page > max_pages:
            break
        all_jobs.extend(jobs_on_page)

    if all_jobs:
        jobs_df = pd.DataFrame(all_jobs)
        save_jobs_to_csv(jobs_df, csv_file)
        print(f"{len(all_jobs)} nuevos trabajos añadidos al archivo CSV.")
    else:
        print("No se encontraron nuevos trabajos para añadir al CSV.")

def scrape_jobs_from_page(page, title, location, time_filter, jobs_per_page):
    job_list = []
    start = page * jobs_per_page
    url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={title}&location={location}&start={start}&f_TPR={time_filter}&f_WT=2"
    print(url)
    response = handle_request_with_retries(url)
    
    if response is None or response.status_code != 200:
        print(f"Error al acceder a la página {page + 1}. Saltando esta página.")
        return job_list

    list_soup = BeautifulSoup(response.text, "html.parser")
    page_jobs = list_soup.find_all("li")

    if not page_jobs:
        print(f"No se encontraron trabajos en la página {page + 1}. Terminando el scraping.")
        return job_list

    for job in page_jobs:
        base_card_div = job.find("div", {"class": "base-card"})
        if not base_card_div:
            continue

        job_id = base_card_div.get("data-entity-urn").split(":")[3]

        job_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
        job_response = handle_request_with_retries(job_url, max_retries=3)
        if job_response is None or job_response.status_code != 200:
            continue

        job_soup = BeautifulSoup(job_response.text, "html.parser")
        match_job = extract_job_details_and_match(base_card_div, job_soup, title)
        if match_job:
            job_list.append(match_job)
    
    return job_list

if __name__ == "__main__":
    main()
