package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Load data from external Hive
  *
  * 准备：
  * 1. hadoop hive 环境正常
  * 2. hive 开启 metaStore 服务 hive --service metastore
  * 3.
  *
  */
object SparkSQL07_Test {
  def main(args: Array[String]): Unit = {
    // TODO : 创建运行上下文
    val conf: SparkConf = new SparkConf().setMaster("local[*]")
      .setAppName("SparkSQL07_Test")
    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()
    // TODO : 导入隐式转换 这里的 spark 是刚创建的环境对象 必须用 val 声明
    // import spark.implicits._

    // spark.sql("create table aa(id int)")

     spark.sql("show tables").show()

     spark.sql("show databases").show

//    spark.sql("load data local inpath 'input/id.txt' into table aa")
//    val df: DataFrame = spark.sql("select * from aa")
//    df.show()

    spark.stop()
  }
}
