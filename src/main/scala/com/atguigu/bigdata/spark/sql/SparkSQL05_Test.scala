package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkSQL05_Test {
  def main(args: Array[String]): Unit = {
    // TODO : 创建运行上下文
    val conf: SparkConf = new SparkConf().setMaster("local[*]")
      .setAppName("SparkSQL05_Test")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // TODO : 导入隐式转换 这里的 spark 是刚创建的环境对象 必须用 val 声明
    // import spark.implicits._

    // TODO : SparkSQL 通用的数据读取和保存 默认数据格式为 Parquet

    // TODO : csv format jdbc json load option options orc parquet schema table text textFile
    // TODO : format() 的参数 ： csv jdbc json orc parquet textFile
    // val df: DataFrame = spark.read.load("input\\users.parquet")

    // read json formatted file  spark 要求每行都是一个标准的字符串  而不是整个文件
    // val df: DataFrame = spark.read.format("json").load("input\\user.json")
    // val df: DataFrame = spark.read.json("input\\user.json")

    // val df: DataFrame = spark.sql("select * from parquet.`input\\users.parquet`")

    // read csv file
    val df: DataFrame = spark.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("input\\people.csv")

    df.show()

    df.printSchema()



    // TODO : 保存数据时指定到目录级别即可，文件名会自动生成
    df.write.format("json").mode(SaveMode.Ignore).save("output")

    spark.stop()
  }
}
