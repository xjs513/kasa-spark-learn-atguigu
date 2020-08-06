package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSQL01_Test {
  def main(args: Array[String]): Unit = {
    // TODO : 创建运行上下文
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Test")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // TODO : 导入隐式转换 这里的 spark 是刚创建的环境对象 必须用 val 声明
    import spark.implicits._
    // TODO : 读取数据
    //    val df: DataFrame = spark.read.format("json").load("input\\user.json")
    // TODO : 注册临时试图
    // df.createOrReplaceTempView("user")
    // TODO : SQL
    // spark.sql("select * from user").show()

    // TODO : DSL
    //    df.select("name", "age").show()

    //    df.select('name, 'age).show()
    // TODO : RDD <=> DataFrame
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List(
      (1, "张三", 30),
      (2, "李四", 20),
      (3, "王五", 40)
    ))
    //    rdd.toDF().show()
    //    rdd.toDF("id", "name", "age").show()
    // TODO : RDD <=> DataSet
    val ds: Dataset[User] = rdd.map {
      case (id, name, age) => User(id, name, age)
    }.toDS()

    ds.show()
    // TODO : DataFrame <=> DataSet
    val dsToDf: DataFrame = ds.toDF()

    val dfToDs = dsToDf.as[User]

    dfToDs.show()

    // TODO : 释放资源
    spark.stop()
  }
  case class User(id:Int, var name:String, age:Int)
}

