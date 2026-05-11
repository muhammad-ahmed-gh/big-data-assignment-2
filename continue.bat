@REM prepare folders and insert data
docker exec -it namenode hdfs dfsadmin -safemode leave
docker exec -it namenode hdfs dfs -rm -r /input
docker exec -it namenode hdfs dfs -rm -r /output
docker exec -it namenode hdfs dfs -mkdir /input
docker exec -it namenode hdfs dfs -put /mds.csv /input/

@REM run mapreduce
docker exec -it namenode hadoop jar /mapreduce.jar ARDriver /input /output

@REM get output
docker exec -it namenode mkdir /output
docker exec -it namenode hdfs dfs -get /output/part-r-00000 /output
docker exec -it namenode cat /output/part-r-00000
docker cp namenode:/output/part-r-00000 .

@REM compose down and return
cd compose-images
docker compose down
cd ..
