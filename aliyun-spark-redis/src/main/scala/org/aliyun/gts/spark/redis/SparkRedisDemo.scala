package org.aliyun.gts.spark.redis

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * Author: Shenjiawei
 */
object SparkRedisDemo {

  val appName = "SparkRedisDemo"
  val host = ""
  val port = 6379
  val auth = ""

  def main(args: Array[String]): Unit = {
    val spu = new SparkRedisUtil

    //read KV
    val databaseKV = "0"
    val arrayKeys = Array("name", "age")
    val spark_string = spu.initSparkRedis(appName, host, port, auth, databaseKV)
    val sc_str: SparkContext = spark_string.sparkContext
    val result_string = spu.readKV(sc_str, arrayKeys)
    println("Result_String："+result_string.mkString)
//    //write KV
//    val data = Seq[(String,String)](("age","24"), ("sex","woman"), ("city","nanjing"))
//    spu.writeKV(sc_str, data)
//    println("Write String Successful!")
//
//    //read List
//    val databaseList = "1"
//    val keysPatternList = "2020*"
//    val spark_list = spu.initSparkRedis(appName, host, port, auth, databaseList)
//    val sc_list: SparkContext = spark_list.sparkContext
//    val result_list = spu.readList(sc_list, keysPatternList)
//    for(value <- result_list){
//      println("Result_List："+value)
//    }
//    //write List
//    val listName = "20200810"
//    val data_list = Seq[String]("1004", "1005", "1006")
//    spu.writeList(sc_list, listName, data_list)
//    println("Write List Successful!")
//
//    //read Hash
//    val databaseHash = "2"
//    val keysPatternHash = "hash_map*"
//    val spark_hash = spu.initSparkRedis(appName, host, port, auth, databaseHash)
//    val sc_hash: SparkContext = spark_hash.sparkContext
//    val result_hash = spu.readHash(sc_hash, keysPatternHash)
//    for(value <- result_hash){
//      println("Result_Hash："+value)
//    }
//    //write Hash
//    val hashName = "hash_map_1"
//    val data_hash = Array[(String,String)](("name","Jack"), ("age","15"), ("city","nanjing"))
//    spu.writeHash(sc_hash, hashName, data_hash)
//    println("Write Hash Successful!")
//
//    // DataFrame
//    val databaseDF = "3"
//    val spark_df = spu.initSparkRedis(appName, host, port, auth, databaseDF)
//    val sc_df: SparkContext = spark_df.sparkContext
//    val regex = " "
//    val viewName = "PersonList"
//    val structFields = Array(StructField("name",StringType,true),StructField("age",IntegerType,true)
//      ,StructField("city",StringType,true))
//    val schema: StructType = StructType(structFields)
//    val data_df = Array[String]("Tom 20 nanjing","Bob 20 nanjing","Linda 20 nanjing")
//    spu.dataFrameInit(spark_df,sc_df,schema,data_df,regex,viewName)
//
//    //dataFrame write
//    spark_df.sql(
//      """
//        |CREATE TEMPORARY VIEW person (name STRING, age INT, city STRING)
//        |    USING org.apache.spark.sql.redis OPTIONS (table 'person', key.column 'name')
//      """.stripMargin)
//
//    spark_df.sql(
//      """
//        |INSERT INTO TABLE person
//        |SELECT * FROM PersonList
//      """.stripMargin)
//
//    //dataFrame read
//    val loadedDf = spark_df.sql(s"SELECT * FROM person")
//    loadedDf.show()
  }
}
