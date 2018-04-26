#!/usr/bin/env bash

echo --Deleting all the files at L1, L1 , Existed .....

hdfs dfs -chmod 777 /tssdemo2
hdfs dfs -rm -r /tssdemo2/*
#hdfs dfs -rm -r /tssdemo2/l1
#hdfs dfs -rm -r /tssdemo2/l2/avro/emp1.avro
#hdfs dfs -rm -r /tssdemo2/l2/parquet/emp1.parquet

echo --Checking if the Directories are cleaned .....

hdfs dfs -ls -R /tssdemo2/