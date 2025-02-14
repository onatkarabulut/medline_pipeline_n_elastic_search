from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import time
import logging
import string
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pipeline.producer import KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MedlineScraperProject:
    def __init__(self):
        self.base_url = "https://medlineplus.gov/druginfo"
        self.producer = KafkaProducer()
        self.drug_links = set()

    def getCategories(self):
        letters = string.ascii_uppercase
        result = [self.base_url + "/drug_{}a.html".format(letter) for letter in letters]
        result.append("https://medlineplus.gov/druginfo/drug_00.html")
        return result

    def getSource(self, url):
        try:
            r = requests.get(url)
            if r.status_code == 200: 
                return BeautifulSoup(r.content, "lxml")
            else:
                logging.error(f"Request failed with status code: {r.status_code}")
                return None
        except requests.RequestException as e:
            logging.error(f"Request error: {e}")
            return None

    def getAllDrugsLinks(self, source):
        if source is None:
            return []

        drug_elements = source.find("ul", attrs={"id": "index"}).find_all("li")
        return set(
            self.base_url + drug.find("a").get("href").replace(".", "", 1) for drug in drug_elements
        )

    def findAllDrugLinks(self):
        categories = self.getCategories()
        for category_link in tqdm(categories, unit=" category link"):
            category_source = self.getSource(category_link)
            result = self.getAllDrugsLinks(category_source)
            self.drug_links.update(result)
        return self.drug_links

    def getSectionInfo(self, source, id_element):
        try:
            section_div = source.find("div", attrs={"id": id_element})
            if not section_div:
                return None
            return {
                "title": section_div.find("h2").text if section_div.find("h2") else "",
                "content": section_div.find("div", attrs={"class": "section-body"}).text if section_div.find("div", attrs={"class": "section-body"}) else ""
            }
        except Exception:
            return None

    def scrapeDrugs(self, start=0, end=None):
        links = list(self.findAllDrugLinks())
        if end is None:
            end = len(links)

        scraped_data = []
        for link in tqdm(links[start:end], unit=" drug link"):
            drug_source = self.getSource(link)
            if drug_source is None:
                continue

            drug_data = {
                "name": drug_source.find("h1", attrs={"class": "with-also"}).text if drug_source.find("h1", attrs={"class": "with-also"}) else None,
                "url": link,
                "sections": [self.getSectionInfo(drug_source, sec) for sec in ["why", "how", "other-uses", "precautions", "special-dietary", "side-effects", "overdose", "other-information", "brand-name-1"]]
            }

            scraped_data.append(drug_data)
            self.sendKafka(drug_data)
            time.sleep(0.5)

        self.flushKafka()
        return scraped_data

    def sendKafka(self, message={}, topic_name="medline-drugs"):
        self.producer.produce(topic_name, json.dumps(message, ensure_ascii=False))

    def flushKafka(self):
        self.producer.flush()
        logger.info("All messages produced successfully")

def run_scraper(**kwargs):
    scraper = MedlineScraperProject()
    start = kwargs.get('start', 0)
    end = kwargs.get('end', None)

    data = scraper.scrapeDrugs(start, end)
    
    # Sonuçları Airflow XCom'a kaydet
    return json.dumps(data)

# Airflow DAG Tanımı
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'medline_scraper_dag',
    default_args=default_args,
    description='Medline scraper using Airflow DAG',
    schedule_interval=None,  # Manuel çalıştırılacak
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    scraping_task = PythonOperator(
        task_id='run_scraper',
        python_callable=run_scraper,
        op_kwargs={'start': 0, 'end': 3}  # Burada start-end parametreleri ayarlanabilir
    )

scraping_task
