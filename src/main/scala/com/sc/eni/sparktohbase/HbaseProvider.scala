package com.sc.eni.sparktohbase

import com.cloudera.spark.hbase.HBaseContext
import com.sc.eni.transformation.SparkProvider
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Admin, ConnectionFactory}

trait HbaseProvider extends SparkProvider{

  val hconf = HBaseConfiguration.create()
  hconf.addResource(new Path("/etc/hbase/conf/core-site.xml"))
  hconf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))

  val hbaseContext = new HBaseContext(sc, hconf)

  //create connection
  val conn = ConnectionFactory.createConnection(hconf)

  //create Admin
  val ad: Admin = conn.getAdmin

}
