package com.aliyun.gts.spark.datahub

import org.apache.spark.sql.aliyun.datahub.DatahubSourceProvider
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
 *
 * Author: wanglei
 * Create At: 2020/8/18 5:57 PM
 */
object SparkDataHubWriterDemo extends App {
      val accessKeyId = ""
      val accessKeySecret = ""
      val endpoint = "http://dh-cn-hangzhou.aliyuncs.com"
      val projectName = ""
      val topicName = ""

      val defaultSchema = StructType(Array(StructField("id", StringType), StructField("name", StringType)))
      val data = Seq(
            Row("1", "wanglei"),
            Row("2", "wangqiao"),
            Row("3", "baixia")
      )
      val sparkSession = SparkSession.builder.appName("Spark write to datahub").getOrCreate()
      val df = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(data), defaultSchema)
      df.write.format("datahub")
        .option(DatahubSourceProvider.OPTION_KEY_ACCESS_ID, accessKeyId: String)
        .option(DatahubSourceProvider.OPTION_KEY_ACCESS_KEY, accessKeySecret: String)
        .option(DatahubSourceProvider.OPTION_KEY_ENDPOINT, endpoint: String)
        .option(DatahubSourceProvider.OPTION_KEY_PROJECT, projectName: String)
        .option(DatahubSourceProvider.OPTION_KEY_TOPIC, topicName: String)
        .mode("append")
        .save()
}
