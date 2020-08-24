package com.aliyun.gts.spark.tablestore

import breeze.numerics.abs
import org.apache.spark.sql.{DataFrame, Row}
import scala.concurrent.forkjoin.ForkJoinPool

/**
 * Author: Shenjiawei
 */
object SparkTableStoreDemo {

  val appName = "SparkTableStoreDemo"
  val source = ""
  val instanceName = ""
  val endpoint = ""
  val accessId = ""
  val accessKey = ""
  val splitSize = "100"
  val mode = "append"

  def main(args: Array[String]): Unit = {

    val sts = new SparkTableStoreUtil()
    val spark = sts.initSparkTableStore(appName)

    // tablestore表结构
    val catalog =  s"""{
                      | "columns":{
                      |   "salt":{"type":"INTEGER"},
                      |   "data_date":{"type":"STRING"},
                      |   "mp_id":{"type":"INTEGER"}
                      | }
                      |}""".stripMargin

    val tableName_source = "a_mp_energy_day_60a"
    val tablestore_source = sts.loadTabelStore(spark,catalog,source,instanceName,tableName_source,
                      endpoint,accessId,accessKey,splitSize)
    tablestore_source.createTempView("OTS_TABLE_SOURCE")

    val tableName_out = "a_mp_energy_day_60b"
    val tablestore_out = sts.loadTabelStore(spark,catalog,source,instanceName,tableName_out,
      endpoint,accessId,accessKey,splitSize)
    tablestore_out.createTempView("OTS_TABLE_OUT")

    // 自定义udf,hash取余获取分区键值
    spark.udf.register("salt_hash", (str: String, salt_num:Int) => abs(str.hashCode%salt_num))
    // sql方式插入tablestore
    spark.sql("insert into OTS_TABLE_OUT select salt_hash(concat_ws('-',data_date,mp_id),60),data_date,mp_id from OTS_TABLE_SOURCE")

    //write tablestore
    val df_out = spark.sql("select * from OTS_TABLE_SOURCE")
    sts.writeTabelStore(df_out,catalog,mode,source,instanceName,tableName_out,
      endpoint,accessId,accessKey,splitSize)

    // 根据主键范围查询
    val totalSalt = 2
    // 串行版本
    var beginTime = System.currentTimeMillis
    var ret = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], tablestore_source.schema)
    for (i <- 0 until totalSalt) {
      val sql = s"select * from OTS_TABLE_SOURCE where salt = ${i} and data_date >= '2020-08-06' and data_date < '2020-08-07'"
      ret = ret.union(spark.sql(sql))
    }
    var endTime = System.currentTimeMillis
    println(s"Serialize version, Total Count: ${ret.count()}, Cost ${endTime-beginTime} ms")

    // Fork-Join并发版本
    beginTime = System.currentTimeMillis
    val sqls = new Array[String](totalSalt)
    for (i <- 0 until totalSalt) {
      sqls(i) = s"select * from OTS_TABLE_SOURCE where salt = ${i} and data_date >= '2020-08-06' and data_date < '2020-08-07'"
    }
    val sqlQueryTask: SqlQueryTask = new SqlQueryTask(spark, tablestore_source.schema, sqls)
    val fjp = new ForkJoinPool()
    val retDF: DataFrame = fjp.invoke(sqlQueryTask)
    endTime = System.currentTimeMillis
    println(s"Fork-Join version, Total Count: ${retDF.count()}, Cost ${endTime-beginTime} ms")

    spark.close()
  }
}
