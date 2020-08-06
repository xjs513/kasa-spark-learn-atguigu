package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * read visa jdbc from MySQL
  */
object SparkSQL06_Test {
  def main(args: Array[String]): Unit = {
    // TODO : 创建运行上下文
    val conf: SparkConf = new SparkConf().setMaster("local[*]")
      .setAppName("SparkSQL06_Test")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // TODO : 导入隐式转换 这里的 spark 是刚创建的环境对象 必须用 val 声明
    // import spark.implicits._

    // TODO : read
    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://dev201:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "Mosh123456")
      .option("dbtable", "emp")
      .load()

    df.show()

    // TODO : write
    df.write.format("jdbc")
      .option("url", "jdbc:mysql://dev201:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "Mosh123456")
      .option("dbtable", "emp_1")
      .mode(SaveMode.Append)
      .save

    spark.stop()
  }
}
