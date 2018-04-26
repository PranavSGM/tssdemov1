package com.sc.eni.sparktohbase

//import java.util
import com.cloudera.spark.hbase.HBaseContext
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

object HbaseWithSpark extends HbaseProvider {


  def main(args: Array[String]) {

    // Checking the arguments are passed properly
      if (args.length < 3) {
        System.out.println("HBaseBulkPutExample {tableName} {columnFamily1} {columnFamily2}")
        return
      }


    // mapping argument to attribute
      val tableName = args(0); //bulkload
      val columnFamily1 = args(1); //personal
      val columnFamily2 = args(2); //prof

    //Reading data from parquet into Df
      val parquetfilepath = "hdfs://quickstart.cloudera:8020/tssdemo2/l2/parquet/emp1.parquet"

    // convert RDD[Row] into RDD[Array[String]]
      val dffromparquet: DataFrame = sqlContext.read.parquet(parquetfilepath)
    println("printDF******")
    dffromparquet.show()
      val inputrdd: RDD[Row] = dffromparquet.rdd
    println("print rdd")
    inputrdd.foreach(println)
      val stringrdd: RDD[String] = inputrdd.map(line => line.toString())
      val toRemove = "[]".toSet
      val correctedrdd: RDD[String] = stringrdd.map(lines => lines.filterNot(toRemove))
      val splitrdd: RDD[Array[String]] = correctedrdd.map(line => line.split(","))

    //Converting RDD[Row] into RDD[columns] of type: (column of Hbase)
    def rowtocolumn(arrays: Array[String]): Array[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])] = {
      val result: Array[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])] = Array(
        (Bytes.toBytes(arrays(0)), Array(
          (Bytes.toBytes(columnFamily1), Bytes.toBytes("empid"), Bytes.toBytes(arrays(0))),
          (Bytes.toBytes(columnFamily1), Bytes.toBytes("empname"), Bytes.toBytes(arrays(1))),
          (Bytes.toBytes(columnFamily1), Bytes.toBytes("emploc"), Bytes.toBytes(arrays(2))),
          (Bytes.toBytes(columnFamily2), Bytes.toBytes("empscore"), Bytes.toBytes(arrays(3))),
          (Bytes.toBytes(columnFamily2), Bytes.toBytes("empoldsal"), Bytes.toBytes(arrays(4))),
          (Bytes.toBytes(columnFamily2), Bytes.toBytes("empnewsal"), Bytes.toBytes(arrays(5)))
      )))
      val fin =result
      fin
    }

    // Input Data : in RDD with two cf1 and two rows :
    // (RowKey, columnFamily, columnQualifier, value)
    val rdd123: RDD[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])] = splitrdd.map(
      lines =>rowtocolumn(lines)
      ).flatMap(x=> x)

   /* val rdd: RDD[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])] =
      sc.parallelize(Array(
        (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("id"), Bytes.toBytes("101")))),
        (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("name"), Bytes.toBytes("ram")))),
        (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily2), Bytes.toBytes("salary"), Bytes.toBytes("12000")))),
        (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("id"), Bytes.toBytes("201")))),
        (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily1), Bytes.toBytes("name"), Bytes.toBytes("sham")))),
        (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily2), Bytes.toBytes("salary"), Bytes.toBytes("15000"))))
      ))
*/
    //If table exists delete it
    deletetableifexists(tableName)

    //create table
    createtable(tableName, columnFamily1, columnFamily2)

    // list the tables
    val listtables = ad.listTables()
    println("Listing the tables- --->")
    listtables.foreach(println)

    // BUlkPut the rdd into HBase table "tableName' -
    println("putting the value into hbase ************")
    bulkputcolumns(hbaseContext,rdd123,tableName)

    // scan table to check if the Hbase table is updated with data - ERROR !!
    //scantable(hbaseContext, tableName)

    // bul Get Hbase data
   // bulkgetcolumns(hbaseContext, tableName, rdd)

  }

  def deletetableifexists ( tableName :String ): Unit ={

    if(ad.tableExists(TableName.valueOf(tableName))){
      if(!ad.isTableDisabled(TableName.valueOf(tableName))){
        ad.disableTable(TableName.valueOf(tableName))
      }
      ad.deleteTable(TableName.valueOf(tableName))
    }

  }

  def createtable(tableName :String,columnFamily1: String, columnFamily2: String): Unit = {
    val tdescriptor: HTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))

    // Adding column families to table descriptor
    tdescriptor.addFamily(new HColumnDescriptor(columnFamily1))
    tdescriptor.addFamily(new HColumnDescriptor(columnFamily2))

    // Execute the table through admin
    ad.createTable(tdescriptor)
  }

  def bulkputcolumns(hBaseContext: HBaseContext, rdd:RDD[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])], tableName: String): Unit = {
println{"entered bulkputcolumns *************"}
    hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](
      rdd,
      tableName,
      (putRecord) => {
        val put = new Put(putRecord._1)
        putRecord._2.foreach((putValue) => put.addColumn(putValue._1, putValue._2, putValue._3))
        put
      },
      "true".toBoolean)
    println{"exiting bulkputcolumns *************"}
  }

/*  def bulkgetcolumns(hbaseContext: HBaseContext, tableName: String, rdd :RDD[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])]): Unit ={


    val getRdd: RDD[String] = hbaseContext.bulkGet[Array[Byte], String](
    //val getRdd: RDD[String] = hbaseContext.bulkGet[Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])], String](
      tableName,
      5,
      rdd.map(v=>v._1),
    //rdd,
      (record) => {
        System.out.println("making Get")
        new Get(record)
      },
      (result: Result) => {

        //val it = result.list().iterator()
        val it = result.listCells().iterator()
        val b = new StringBuilder

        //getting the rowkey of the row of result
        b.append(Bytes.toString(result.getRow()) + ":")

        while (it.hasNext()) {
          val kv = it.next()
          //val q = Bytes.toString(kv.getQualifierArray())
          val q = Bytes.toString(kv.getQualifier())
          if (q.equals("name")) {
            //b.append("(" + Bytes.toString(kv.getQualifierArray()) + "," + Bytes.toLong(kv.getValueArray()) + ")")
            b.append("(" + Bytes.toString(kv.getQualifier()) + "," + Bytes.toString(kv.getValue()) + ")")
          } else {
            //b.append("(" + Bytes.toString(kv.getQualifierArray()) + "," + Bytes.toString(kv.getValueArray()) + ")")
            b.append("(" + Bytes.toString(kv.getQualifier()) + "," + Bytes.toLong(kv.getValue()) + ")")
          }
        }
        val x: String = b.toString
        x
      })


    getRdd.collect.foreach(v => println(v))

  }*/
/*  def scantable(hBaseContext:HBaseContext, tableName : String ): Unit = {

    val scan = new Scan()
    scan.setCaching(100)

    var getRdd: RDD[(Array[Byte], util.List[(Array[Byte], Array[Byte], Array[Byte])])] =
      hbaseContext.hbaseScanRDD(tableName, scan)

    //println("number of entries in table : " + tableName +" = > \n " +getRdd.count() )
    getRdd.foreach(v => println(Bytes.toString(v._1)))
    //getRdd.collect().foreach(println)
    /*println(" --- abc")
    getRdd.foreach(v => println(Bytes.toString(v._1)))
    println(" --- def")
    getRdd.collect.foreach(v => println(Bytes.toString(v._1)))
    println(" --- qwe")*/

  }*/

}
