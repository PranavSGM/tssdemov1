#!/usr/bin/env bash

echo --Starting Services for HDFS, If Required .....

sudo service hadoop-hdfs-datanode start
sudo service hadoop-hdfs-journalnode start
sudo service hadoop-hdfs-namenode start
sudo service hadoop-hdfs-secondarynamenode start
sudo service hadoop-httpfs start








