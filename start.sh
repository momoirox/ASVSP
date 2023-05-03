sudo docker exec -it spark-master pip3 install requests

sudo docker exec -it spark-master ./spark/bin/spark-submit  ../Producer/Batch/app.py
sudo docker exec -it spark-master ./spark/bin/spark-submit  --jars ../Consumer/postgresql-42.5.1.jar ../Consumer/Batch/batch.py 
docker exec -it spark-master ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars ../Consumer/postgresql-42.5.1.jar ../Consumer/Rel/real_time.py



