# spark base image
# borrowed from: https://github.com/brunocfnba/docker-spark-cluster

FROM ubuntu:16.04

RUN apt-get update && apt-get install -y \
 wget \
 default-jdk \
 python \
 nano

RUN cd /home && mkdir spark && cd spark && \
 wget https://archive.apache.org/dist/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.6.tgz && \
 tar -xvf spark-2.3.0-bin-hadoop2.6.tgz && \
 wget https://www.scala-lang.org/files/archive/scala-2.11.12.deb && \
 dpkg -i scala-2.11.12.deb

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-amd64 
ENV SPARK_HOME /home/spark/spark-2.3.0-bin-hadoop2.6