package com.atguigu.bigdata.spark.sql

import com.atguigu.bigdata.spark.sql.SparkSQL01_Test.User
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

/**
  * UDAF 强类型 简单版 加上类型
  */
object SparkSQL04_Test {

  def main(args: Array[String]): Unit = {
    // TODO : 创建运行上下文
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL04_Test")
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
    // TODO :  创建自定义函数
    val udaf = new MyAvgAgeUDAFClass
    // 因为聚合函数是强类型，但是 SQL 中没有类型的概念，所以无法使用 DS 有类型的概念
    // TODO :  使用自定义函数 可以采用 DSL 语法进行访问
    val value: Dataset[Long] = userDS.select(udaf.toColumn)
    value.show()
    spark.stop()
  }


  /**
    * TODO : 定义 UDAF 函数 强类型
    * 1. 继承 Aggregator,定义泛型
    *    -IN : 输入数据类型 User
    *    BUF : 缓冲区数据类型 AvgBuffer
    *    OUT : 输出数据类型 Long
    * 2. 重写方法 6 个
    */
  class MyAvgAgeUDAFClass extends Aggregator[User, AvgBuffer, Long]{

    // TODO : 1. 缓冲区初始化
    override def zero: AvgBuffer = AvgBuffer(0L, 0L)
    // TODO : 2. 聚合数据
    override def reduce(b: AvgBuffer, a: User): AvgBuffer = {
      AvgBuffer(b.totalAge + a.age, b.count + 1L)
    }
    // TODO : 3. 缓冲区合并
    override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
      AvgBuffer(b1.totalAge + b2.totalAge, b1.count + b2.count)
    }
    // TODO : 4. 计算最终结果
    override def finish(ab: AvgBuffer): Long = ab.totalAge / ab.count


    // 编码方式 自定义类固定写法
    override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product
    // 编码方式 原生数据类型采用语言自带的
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

  case class AvgBuffer(var totalAge:Long, var count:Long)
}
