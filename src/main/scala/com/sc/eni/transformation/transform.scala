package com.sc.eni.transformation

import java.io.File

import com.databricks.spark.avro._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

object transform extends SparkProvider {

  def main(args:Array[String]) = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val inputfilepath = "hdfs://quickstart.cloudera:8020/tssdemo2/l1/csv/emp1.csv"
    val avrofilepath = "hdfs://quickstart.cloudera:8020/tssdemo2/l2/avro/emp1.avro"
    val parquetfilepath = "hdfs://quickstart.cloudera:8020/tssdemo2/l2/parquet/emp1.parquet"

    val filerdd : RDD[String] = sc.textFile(inputfilepath)
    println("LOG_INFO : Printing the input RAW csv file ..... ")
    println(filerdd.foreach(println))

    /*transformation logic */
    val emprdd2 = transformation(filerdd)

    /* Convert to Dataframe */
    import sqlContext.implicits._
    val df:DataFrame  = emprdd2.toDF("empno","empname","emploc","empscore","empoldsalary","empnewsalary")
    println("LOG_INFO : Printing the transformed data as a DATAFRAME ..... ")
    df.show()

    //convert to avro file and save to l2 at hdfs
    writetoavro(df,avrofilepath)

    //Read avro from l2 and convert to df
    readfromavro(sqlContext,avrofilepath)

    //Convert Df to parquet file at L3 on HDFS
    writetoparquet(df, parquetfilepath)

    //convert paquet file to Df to check the data
    readfromparquet(sqlContext, parquetfilepath)
  }

  def transformation( filerdd:RDD[String]): RDD[(String,String,String,Int,Float,Float)] ={
    val emprdd = filerdd.map {
      line =>
        val col = line.split(",")
        (col(0), col(1), col(2), col(3).toInt, col(4).toFloat)
    }.map{
      case(empno,empname,emploc,empscore, empsalary) =>
        if(empscore >= 85) (empno, empname, emploc, empscore, empsalary, "%.2f".format(empsalary*(1.25)).toFloat)
        else if(empscore >=70) (empno, empname, emploc, empscore, empsalary, "%.2f".format(empsalary*(1.20)).toFloat)
        else if(empscore >=55) (empno, empname, emploc, empscore, empsalary, "%.2f".format(empsalary*(1.15)).toFloat)
        else  (empno, empname, emploc, empscore, empsalary,  "%.2f".format(empsalary*(1.05)).toFloat)
    }
    println("\nLOG_INFO : Printing the Transformed RDD ..... ")
    println(emprdd.foreach(println))
    emprdd
  }

  def writetoavro (df: DataFrame, filepath:String): Unit ={

    val f = new File(filepath)
    df.write.mode(SaveMode.Overwrite).avro(filepath)
    println("\nLOG_INFO : Transformed Data is written in AVRO file format at Dir : "+filepath+" ..... ")

    //      try {
//        df.write.avro("src/main/resources/avro/emp2.avro")
//      } catch {
//        case e: IOException => println("LOG_INFO : Got an IOException : " + e)
//        case e: Exception => println("LOG_INFO : Got Exception : " + e)
//      } finally{
//        if(new File("src/main/resources/avro/emp2.avro").exists())
//          println("LOG_INFO : AVRO file is written to path : src/main/resources/avro/emp2.avro ")
//      }
  }

  def readfromavro(sqlContext: SQLContext, filepath:String): Unit ={
    val dfavro = sqlContext.read
      .format("com.databricks.spark.avro")
        .load(filepath)
    println("\nLOG_INFO : CHECKING TRANSFORMED AVRO FILE ..... \nLOG_INFO : Reading Avro file at path : "+filepath+" ..... ")
    dfavro.show()
  }

  def writetoparquet(frame: DataFrame, filepath:String): Unit ={
    frame.write.parquet(filepath)
    println("\nLOG_INFO : Transformed Data is written in PARQUET file format at Dir : "+filepath+" ..... ")
  }

  def readfromparquet(context: SQLContext, filepath:String): Unit ={
    val dffromparquet: DataFrame = context.read.parquet(filepath)
    println("\nLOG_INFO : CHECKING TRANSFORMED Parquet FILE ..... \nLOG_INFO : Reading Parquet file at path : "+filepath+" ..... ")
    dffromparquet.show()
  }


}
