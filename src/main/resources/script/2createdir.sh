#!/usr/bin/env bash

echo -- Creating Directories in HDFS .....

hdfs dfs -mkdir /tssdemo2
hdfs dfs -mkdir /tssdemo2/l1
hdfs dfs -mkdir /tssdemo2/l1/csv
hdfs dfs -mkdir /tssdemo2/l2
hdfs dfs -mkdir /tssdemo2/l2/avro
hdfs dfs -mkdir /tssdemo2/l2/parquet

echo !! Created Dir : L1 , L2 at /tssdemo2 on HDFS !

hdfs dfs -chmod -R 777 /tssdemo2

echo -- Listing all the Created Directories .....

hdfs dfs -ls -R /tssdemo2
