docker run -it --rm \
  --name mydev \
  -e ENABLE_INIT_DAEMON=false \
  --env-file ./config/hadoop-hive.env \
  -v ~/bdi/resources/:/bdi/resources/ \
  -v /home/lucas/bdidemo/src/:/app/src/ \
  --network bdi_net_lucas \
  --link spark-master:spark-master  \
  lucas/myapp \
  /spark/bin/spark-shell --jars /bdi/resources/mysql-connector-java-8.0.22.jar,/bdi/resources/kudu-spark3_2.12-1.13.0.7.1.5.2-1.jar  --driver-class-path /bdi/resources/mysql-connector-java-8.0.22.jar
