package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * UDF
  */
object SparkSQL02_Test {
  def main(args: Array[String]): Unit = {
    // TODO : 创建运行上下文
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Test")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // TODO : 导入隐式转换 这里的 spark 是刚创建的环境对象 必须用 val 声明
    import spark.implicits._

    // TODO : RDD <=> DataFrame
    val rdd: RDD[(Int, String, Int,String)] = spark.sparkContext.makeRDD(List(
      (1, "张三", 30, "BJ"),
      (2, "李四", 20, "HN"),
      (3, "王五", 40, "XJ")
    ))
    val userDS: Dataset[User] = rdd.toDF("id", "name", "age", "city").as[User]

    // TODO : SparkSQL 封装的对象提供了大量的方法进行数据处理，类似 RDD 的算子
    // val resultDS: Dataset[User] = userDS.map(user => {user.name = "name : " + user.name; user})
    userDS.createOrReplaceTempView("user")
    spark.udf.register("addPrefixString", (x:String, y:String) => y+x)

    val frame: DataFrame = spark.sql("select id + 1 as id, addPrefixString(name, 'ddd') as fullName, age, city from user")

    frame.show()

    // resultDS.show()
    // userDS.printSchema()
    spark.stop()
  }
}
