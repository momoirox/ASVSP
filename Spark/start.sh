sudo docker exec -it spark-master pip3 install requests

sudo docker exec -it spark-master ./spark/bin/spark-submit  ../data/app.py &
sudo docker exec -it spark-master ./spark/bin/spark-submit  ../Producer/Batch/load_movies.py 

