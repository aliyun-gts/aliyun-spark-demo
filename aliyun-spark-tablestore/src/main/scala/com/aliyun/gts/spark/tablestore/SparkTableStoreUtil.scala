package com.aliyun.gts.spark.tablestore

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author: Shenjiawei
 */
class SparkTableStoreUtil {

  // 初始化Spark TableStore
  def initSparkTableStore(appName: String): SparkSession ={
    val spark = SparkSession.builder
      .appName(appName)
      .getOrCreate()
    spark
  }

  // load tablestore
  def loadTabelStore(spark: SparkSession, catalog: String, source: String,
                     instanceName: String, tableName: String, endpoint: String,
                     accessId: String, accessKey: String, splitSize: String): DataFrame ={

    val dataFrame: DataFrame = spark.read
      .format(source)
      .option("catalog", catalog)
      .option("instance.name", instanceName)
      .option("table.name", tableName)
      .option("endpoint", endpoint)
      .option("access.key.id", accessId)
      .option("access.key.secret", accessKey)
      .option("split.siaz.mbs", splitSize)
      .load()
    dataFrame
  }

  // write tablestore
  def writeTabelStore(dataFrame: DataFrame, catalog: String, mode: String, source: String,
                     instanceName: String, tableName: String, endpoint: String,
                     accessId: String, accessKey: String, splitSize: String): Unit = {
    dataFrame.write
        .mode(mode)
        .format(source)
        .option("instance.name", instanceName)
        .option("table.name", tableName)
        .option("endpoint", endpoint)
        .option("access.key.id", accessId)
        .option("access.key.secret", accessKey)
        .option("catalog", catalog)
        .save()
  }
}
