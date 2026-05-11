cd compose-images
docker compose up -d

@REM copy data and jar file
cd ../mapreduce
docker cp mds.csv namenode:/
docker cp out/artifacts/mapreduce_jar/mapreduce.jar namenode:/