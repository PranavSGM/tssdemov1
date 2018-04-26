#!/usr/bin/env bash

echo Listing the Hbase tables...

#hbase shell ../hbase/list.txt

echo --Running the Spark-submit job...

/usr/lib/spark/bin/spark-submit \
  --class com.sc.eni.sparktohbase.HbaseWithSpark \
  --master local \
  ~/workspace/tssdemo2/target/tssdemo2-1.0-SNAPSHOT.jar




echo Listing the Hbase tables...

#hbase shell ../hbase/list.txt