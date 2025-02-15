import string
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import json
import logging
import click
import time
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
        result = list(map(lambda letter: self.base_url + "/drug_{}a.html".format(letter), letters))
        result.append("https://medlineplus.gov/druginfo/drug_00.html")
        return result

    def getSource(self, url):
        r = requests.get(url)
        if r.status_code == 200: 
            return BeautifulSoup(r.content, "lxml")
        else:
            logging.error("Your status code == {}".format(r.status_code))
            return None

    def getAllDrugsLinks(self, source):
        """ 
        html#drug-index.us > body > div#mplus-wrap > div#mplus-content > article > ul#index 
        """
        if source is None:
            return []

        drug_elements = source.find("ul", attrs={"id":"index"}).find_all("li")
        drug_links = list(map(
            lambda drug: self.base_url + drug.find("a").get("href").replace(".", "", 1), 
            drug_elements
        ))
        return set(drug_links)

    def findAllDrugLinks(self):
        categories = self.getCategories()
        bar = tqdm(categories, unit=" category link")
        for category_link in bar:
            bar.set_description(category_link)
            category_source = self.getSource(category_link)
            result = self.getAllDrugsLinks(category_source)
            self.drug_links = self.drug_links.union(result)
        return self.drug_links

    def getName(self, source):
        try:
            return source.find("h1", attrs={"class":"with-also"}).text
        except Exception:
            return None

    def getSectionInfo(self, source, id_element):
        try:
            section_div = source.find("div", attrs={"id": id_element})
            if not section_div:
                return None
            title = section_div.find("h2").text if section_div.find("h2") else ""
            content_div = section_div.find("div", attrs={"class": "section-body"})
            content = content_div.text if content_div else ""
            return dict(
                title=title,
                content=content
            )
        except Exception:
            return None

    def scrapeDrugs(self, start, end):
        if start is None:
            start = 0

        links = list(self.findAllDrugLinks())
        if end is None:
            end = len(links)

        bar = tqdm(links[start:end], unit=" drug link")
        scraped_data = []
        
        for link in bar:
            bar.set_description(link)
            drug_source = self.getSource(link)
            if drug_source is None:
                continue

            name = self.getName(drug_source)
            sections = []
            why = self.getSectionInfo(drug_source, "why")
            how = self.getSectionInfo(drug_source, "how")
            other_uses = self.getSectionInfo(drug_source, "other-uses")
            precautions = self.getSectionInfo(drug_source,"precautions")
            special_dietary = self.getSectionInfo(drug_source,"special-dietary")
            side_effects = self.getSectionInfo(drug_source,"side-effects")
            overdose = self.getSectionInfo(drug_source,"overdose")
            other_information = self.getSectionInfo(drug_source,"other-information")
            brand_name_1 = self.getSectionInfo(drug_source,"brand-name-1")

            sections.extend([
                why, how, other_uses, precautions, 
                special_dietary, side_effects, overdose, 
                other_information, brand_name_1
            ])

            drug_data = dict(
                name=name,
                url=link,
                sections=sections
            )

            scraped_data.append(drug_data)
            self.sendKafka(message=drug_data)
            time.sleep(0.5)

        self.flushKafka()
        return scraped_data

    def sendKafka(self, message={},topic_name = "medline-drugs"):
        # logger.info(f"Sendded -->{str(message.get("url"))}\n" )
        self.producer.produce(topic_name, json.dumps(message, ensure_ascii=False))

    def flushKafka(self):
        self.producer.flush()
        logger.info("All messages produced successfully")

    def jsonWriter(self, data, filename):
        with open(filename, "w", encoding="utf-8") as file:
            file.write(json.dumps(data, indent=2, ensure_ascii=False))

@click.command()
@click.option("--start",
              type=int,
              help="[EN] Where does it start?\n[TR] Nereden başlasın?",
              default=0)
@click.option("--end",
              type=int,
              help="[EN] Where does it end?\n[TR] Nerede dursun?",
              )
@click.option("--filename",
              help="[EN] Enter a filename.\n[TR] Dosya adı giriniz.",
              default="./scraping/json_data/scraping_data.json",
              type=str)

def run(start, end, filename="./scraping/json_data/scraping_data.json"):
    scraper = MedlineScraperProject()
    data = scraper.scrapeDrugs(start, end)
    scraper.jsonWriter(data, filename)

if __name__ == '__main__':
    run()
