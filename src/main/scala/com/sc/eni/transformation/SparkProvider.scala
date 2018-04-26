package com.sc.eni.transformation

import org.apache.spark.{SparkConf, SparkContext}

trait SparkProvider {

  val conf = new SparkConf()
    .setAppName("transform")
    .setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

}
