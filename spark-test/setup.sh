#!/bin/bash
#`1. Install hadoop `
wget https://archive.apache.org/dist/hadoop/core/hadoop-3.2.1/hadoop-3.2.1.tar.gz
tar -xvzf  hadoop-3.2.1.tar.gz

#2. Install spark
wget https://archive.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-without-hadoop.tgz
tar -xvzf spark-2.4.4-bin-without-hadoop.tgz


cp lib/*  spark-2.4.4-bin-without-hadoop/jars/
