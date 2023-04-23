#sudo docker exec -it spark-master pip3 install requests

#sudo docker exec -it spark-master ./spark/bin/spark-submit  ../Producer/Batch/app.py
sudo docker exec -it spark-master ./spark/bin/spark-submit  --jars ../Consumer/Batch/postgresql-42.5.1.jar ../Consumer/Batch/batch.py 


