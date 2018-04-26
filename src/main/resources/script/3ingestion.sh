#!/usr/bin/env bash

echo TSS/MVP-DEMO Project

#Ingesting the csv file from local Unix Cloudera machine to HDFS
echo -- Before Ingetion .....

hdfs dfs -ls /tssdemo2/l1/csv

echo -- Ingesting : ~/workspace/tssdemov2/src/main/resources/csv/emp1.csv TO /tssdemo2/l1/csv .....

hdfs dfs -put ~/workspace/tssdemov2/src/main/resources/csv/emp1.csv /tssdemo2/l1/csv

echo !! Ingestion Process Completed Successfully !

hdfs dfs -chmod 777 /tssdemo2/l1/csv/emp1.csv

echo -- Listing the ingested file in HDFS .....

hdfs dfs -ls /tssdemo2/l1/csv

