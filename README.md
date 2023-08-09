# Flink-For-Statement

Steps to run this code - 

1. In terminal navigate to the working directory of this project.
2. Run - "docker-compose up". This will create container in docker.
3. Then run - "docker build -t my-flink-app:latest .". This will create image in your docker that will connect to flink app.
4. Install minio on your system then run - "minio server start". This will start minio server to connect with this flink application.
5. Then using ip address of your local system you will connect to minio. Change minioendpoint with ip address of your system and change port with the port mentioned in the minio server port (will be showing in the terminal where you ran minio server start command).
6. Create two buckets in minio dashboard named - "bucket-for-statement" and "statement-result".
7. Create jar file of your code using command - "mvn clean package".
8. Upload this jar file in your apache flink dashboard and run your jar file and VIOLA your code ran successfully.
