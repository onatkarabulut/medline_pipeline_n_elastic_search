### create conda env and activate ()zorunlu
conda env create -f environment.yml
conda activate medline_search

()zorunlu
echo -e "AIRFLOW_UID=$(id -u)" > .env
AIRFLOW_UID=50000
docker-compose up airflow-init

### compose up in silent mode()zorunlu
sudo docker-compose up

### wait until process complete()zorunlu!!!!!

### Create a topic()zorunlu!!!!!!
sudo docker exec kafka-broker-1 kafka-topics --create \
  --topic medline-drugs \
  --partitions 2 \
  --replication-factor 2 \
  --bootstrap-server kafka-broker-1:9092



### Topic List
sudo docker exec kafka-broker-1 kafka-topics --list --bootstrap-server kafka-broker-1:9092

### start fastapi()zorunlu
fastapi run api/app.py --reload

### start consumer()zorunlu
python3 pipeline/consumer.py

### activate the scraper_DAG()zorunlu 
python3 dags/scraper_DAG.py

# if you wanna get a connect to psql
sudo docker exec -it medline_pipeline_n_elastic_search_postgres_1 psql -U airflow -d airflow



