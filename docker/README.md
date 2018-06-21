## Docker spiking

**Create base image**

`$ docker build --t spark-base .`

**Run the `master` node**

```
$ docker run -p 9999:8080 -p 9997:7077 --rm --name spark-master \
	--entrypoint "/home/spark/spark-2.3.0-bin-hadoop2.6/bin/spark-class" \
	spark-base \
	org.apache.spark.deploy.master.Master
```

_The UI will be available at http://localhost:9999/_

**Run as many `slave`s (workers) nodes as you want to**

```
$ docker run --rm --link spark-master:spark-master \
	--entrypoint "/home/spark/spark-2.3.0-bin-hadoop2.6/bin/spark-class" \
	spark-base \
	org.apache.spark.deploy.worker.Worker spark://spark-master:7077
```

**Run the spark-submit with some example**

_from the root instalation folder, on host machine_

```
$ ./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master spark://localhost:9997 \
  --executor-memory 1G \
  --total-executor-cores 4 \
  ./examples/jars/spark-examples_2.11-2.3.0.jar \
  1000
 ```

_inside the container_

 ```
 $ docker inspect --format '{{ .NetworkSettings.IPAddress }}' spark-master
 $ docker exec -it spark-master bash
 # cd /home/spark/spark-2.3.0-bin-hadoop2.6
 # ./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master spark://172.17.0.3:7077 \
  --executor-memory 1G \
  --total-executor-cores 4 \
  ./examples/jars/spark-examples_2.11-2.3.0.jar \
  1000
  ```