#!/usr/bin/env bash

sudo service impala-state-store start
sudo service impala-catalog start
sudo service impala-server start

hdfs dfs -chmod -R 777 /tssdemo2/

hdfs dfs -rm -r /user/hive/warehouse/emp1

echo --Runing Impala-shell queries for creating and loading data to parquet table in impala .....

impala-shell -i localhost -f ~/workspace/tssdemov2/src/main/resources/sql/impala.sql

echo --Display Complete table content for emp1;

impala-shell -q 'select * from emp1;';

echo --Query-1 : Find the list of employee scoring 75 and more .....

impala-shell -q 'select * from emp1 where empscore>=75;'

echo --Query-2 : Find the Total Increase in Salary per employee  .....

impala-shell -q 'select Sum(empnewsalary-empoldsalary)/count(*) As TotalIncrement from emp1;'
