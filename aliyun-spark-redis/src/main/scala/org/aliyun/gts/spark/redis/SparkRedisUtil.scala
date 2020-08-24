package org.aliyun.gts.spark.redis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Author: Shenjiawei
 */
class SparkRedisUtil {

  // 初始化Spark Redis
  def initSparkRedis(appName: String, host: String, port :Int,
                     auth : String, databaseNum: String): SparkSession ={
    val spark = SparkSession
      .builder()
      .appName(appName)
      .config("spark.redis.host", host)
      .config("spark.redis.port", port)
      .config("spark.redis.auth", auth)
      .config("spark.redis.db",databaseNum)
      .getOrCreate()
    spark
  }

  //读取String类型数据
  def readKV(sparkContext: SparkContext, array: Array[String]): String ={
    import com.redislabs.provider.redis._
    sparkContext.fromRedisKV(array).collect().mkString
  }

  //写入String类型数据
  def writeKV(sparkContext: SparkContext, data:Seq[(String, String)]): Unit ={
    import com.redislabs.provider.redis._
    sparkContext.toRedisKV(sparkContext.parallelize(data))
  }

  //读取List类型数据
  def readList(sparkContext: SparkContext, keysPatternList: String): Array[String] ={
    import com.redislabs.provider.redis._
    sparkContext.fromRedisList(keysPatternList).collect()
  }

  //写入List类型数据
  def writeList(sparkContext: SparkContext, listName: String, data: Seq[String]): Unit ={
    import com.redislabs.provider.redis._
    sparkContext.toRedisLIST(sparkContext.parallelize(data), listName)
  }

  //读取Hash类型数据
  def readHash(sparkContext: SparkContext, keysPatternHash: String): Array[(String, String)] ={
    import com.redislabs.provider.redis._
    sparkContext.fromRedisHash(keysPatternHash).collect()
  }

  //写入Hash类型数据
  def writeHash(sparkContext: SparkContext, hashName: String, array: Array[(String,String)]): Unit ={
    import com.redislabs.provider.redis._
    sparkContext.toRedisHASH(sparkContext.parallelize(array), hashName)
  }

  // 初始化DataFrame表
  def dataFrameInit(spark: SparkSession, sparkContext: SparkContext, schema: StructType,
                    data: Array[String], regex:String, viewName: String): Unit ={
    val row: RDD[Array[String]] = sparkContext.parallelize(data).map(_.split(regex))
    val rowRDD: RDD[Row] = row.map(t => {
      var row: Row = Row()
      for (i <- t.indices) {
        if(i==1)row = Row.merge(row,Row(t(i).toInt))
        else row = Row.merge(row,Row(t(i)))
      }
      row
    })
    spark.createDataFrame(rowRDD,schema).createTempView(viewName)
  }
}
