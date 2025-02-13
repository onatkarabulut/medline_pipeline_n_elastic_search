### create conda env and activate
conda env create -f environment.yml
conda activate medline_search


### compose up in silent mode
sudo docker-compose up -d

### wait until process complete

### Create a topic
sudo docker exec kafka-broker-1 kafka-topics --create \
  --topic medline-drugs \
  --partitions 2 \
  --replication-factor 2 \
  --bootstrap-server kafka-broker-1:9092

### Topic List
sudo docker exec kafka-broker-1 kafka-topics --list --bootstrap-server kafka-broker-1:9092


### start
bash start-all.sh

https://medium.com/@Shamimw/steps-to-install-apache-airflow-using-docker-compose-9d663ea2e740



sudo docker exec -it medline_pipeline_n_elastic_search_postgres_1 psql -U airflow -d airflow