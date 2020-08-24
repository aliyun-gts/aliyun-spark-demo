package com.aliyun.gts.spark.tablestore

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.concurrent.forkjoin.{ForkJoinTask, RecursiveTask}

/**
 * Author: Zhuorang
 * fork-join并发编程
 */
class SqlQueryTask(spark: SparkSession, schema: StructType, sqls: Array[String]) extends RecursiveTask[DataFrame] {
  override def compute(): DataFrame = {
    var ret = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    if (sqls.length == 0) {
      return spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    }
    if (sqls.length == 1) {
      val df: DataFrame = spark.sql(sqls(0))
      return df
    }

    val middleIdx = sqls.length / 2
    val task1 = new SqlQueryTask(spark, schema, sqls.slice(0, middleIdx))
    val task2 = new SqlQueryTask(spark, schema, sqls.slice(middleIdx, sqls.length))

    // fork 子任务
    ForkJoinTask.invokeAll(task1, task2)

    // join子任务
    val join1 = task1.join()
    val join2 = task2.join()

    ret = ret.union(join1)
    ret = ret.union(join2)
    ret
  }
}
