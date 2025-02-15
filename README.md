# Medline Pipeline & ElasticSearch

![Medline Search Architecture](medline_search.drawio(1).png)

Welcome to the Medline Pipeline & ElasticSearch project! This project is designed to scrape drug data from MedlinePlus, send the data in real-time to two Kafka brokers, and then index the data in Elasticsearch. You can also interact with the data using a FastAPI service and perform fuzzy searches with a command-line tool.

## What Does This Project Do?

- **Scraping:**  
  The project scrapes drug information from MedlinePlus. For example, you can run:
  
  ```bash
  $ python3 scraping.py --end 10
  #check out --> scraping/README.md

This will scrape 10 drug records.

  *  Data Flow:
    The scraped data is sent to two Kafka brokers. From there, a Kafka consumer picks up the data and indexes it into Elasticsearch.

  *  API Access:
    A FastAPI service is provided so you can interact with Elasticsearch. You can use the API to perform queries, aggregations, scrolling searches, and more. (http://127.0.0.1:8000/docs#/)

  *  Search:
    Once the drug data is indexed, you can perform fuzzy searches using:

    $ python3 search_cli.py --query "Pentamidine injection" --top_n 10

  *  This command displays the search results and saves them as a JSON file.

### How to Run the Project

Before you start, please ensure your computer has the following installed:

    Conda (with Python 3.10)
    Docker & Docker Compose

Starting All Services

You can start all the services by running the provided script:

    $ bash start-all.sh

* This script will:

    Check if the Conda environment (medline_search) exists and create it if needed.
    Activate the Conda environment.
    Start Docker Compose (which runs Kafka, Elasticsearch, Postgres, etc.) and log the output.
    Wait a minute for all services to initialize.
    Create and list the Kafka topic (medline-drugs).
    Start the FastAPI server and Kafka consumer, and save logs in the logs/ folder.

Scraping Data

To scrape data from MedlinePlus, simply run:

    $ python3 scraping.py --end 10

This command scrapes 10 drug records. The scraped data is sent to Kafka and then indexed into Elasticsearch.
Searching the Data

Once the data is indexed, you can search it using the command-line tool:

    $ python3 search_cli.py --query "Pentamidine injection" --top_n 10

This will display the top 10 search results in your terminal and save them as a JSON file (by default, saved to ./elastic_search/results/results.json).

A Quick Recap

  * Start Services: Run bash start-all.sh to launch all services.
  * Scrape Data: Use python3 scraping.py --end 10 to scrape drug data from MedlinePlus.
  * Data Flow: The scraped data is sent to Kafka and then indexed into Elasticsearch.
  * API: FastAPI provides endpoints for various Elasticsearch features. (http://127.0.0.1:8000/docs#/)
  * Search: Query the indexed data with python3 search_cli.py and view/save results as JSON.