package com.aliyun.gts.spark.datahub

import com.aliyun.datahub.model.RecordEntry
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.aliyun.datahub.DatahubUtils

/**
 *
 * Author: wanglei
 * Create At: 2020/8/8 10:37 AM
 */
object SparkDataHubReaderDemo {
  def main(args: Array[String]): Unit = {
    val accessKeyId = ""
    val accessKeySecret = ""
    val endpoint = "http://dh-cn-hangzhou.aliyuncs.com"
    val projectName = ""
    val topicName = ""

    val conf = new SparkConf().setAppName("SparkDataHub").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(1))

    // 对DataHub数据RecordEntry的处理函数，取出RecordEntry中第一个field的数据
    def read(record: RecordEntry): String = {
      record.getString(0)
    }

    val datahubStream = DatahubUtils.createStream(
      ssc,
      projectName, // DataHub Project
      topicName, // DataHub topic
      "1596870533563SJUP3", // DataHub的订阅ID
      accessKeyId,
      accessKeySecret,
      endpoint, // DataHub endpoint
      read: RecordEntry => String, // 对DataHub数据RecordEntry的处理
      StorageLevel.MEMORY_AND_DISK)
    datahubStream.foreachRDD(rdd => println(rdd.count()))
    ssc.start()
    ssc.awaitTermination()

    //    val sparkSession = SparkSession.builder.appName("Spark with datahub").getOrCreate()
    //    val dataHubStream = sparkSession.readStream.format("datahub")
    //      .option("endpoint", endpoint: String)
    //      .option("project", projectName: String)
    //      .option("topic", topicName: String)
    //      .option("access.key.id", accessKeyId: String)
    //      .option("access.key.secret", accessKeySecret: String)
    //      .load()
    //    import sparkSession.implicits._
    //    val records = dataHubStream.select("msg": String).as[String]
    //    val count = records.foreach(println(_))
  }
}
