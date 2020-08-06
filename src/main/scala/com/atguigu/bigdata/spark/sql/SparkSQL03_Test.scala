package com.atguigu.bigdata.spark.sql

import com.atguigu.bigdata.spark.sql.SparkSQL01_Test.User
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * UDAF
  */
object SparkSQL03_Test {
  def main(args: Array[String]): Unit = {
    // TODO : 创建运行上下文
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL03_Test")
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
    // TODO :  注册自定义函数
    val udaf = new MyAvgAgeUDAF
    spark.udf.register("avgAge", udaf)

    val frame: DataFrame = spark.sql("select name, avgAge(age) avgAge from user group by name")

    frame.show()

    // resultDS.show()
    // userDS.printSchema()
    spark.stop()
  }


  /**
    * TODO : 定义 UDAF 函数
    * 1. 继承 UserDefinedAggregateFunction
    * 2. 重写方法 8 个
    */
  class MyAvgAgeUDAF extends UserDefinedAggregateFunction{

    // TODO : 1. 输入数据结构信息 : 年龄信息
    override def inputSchema: StructType = {
      StructType(
        Array(
          StructField("age", LongType, nullable = false)
        )
      )
    }
    // TODO : 2. 缓冲区的数据结构信息 : 年龄总和, 人数
    override def bufferSchema: StructType = {
      StructType(
        Array(
          StructField("totalAge", LongType, nullable = false),
          StructField("count", LongType, nullable = false)
        )
      )
    }
    // TODO : 3. 聚合函数返回结果类型
    override def dataType: DataType = LongType
    // TODO : 4. 聚合函数稳定性
    override def deterministic: Boolean = true
    // TODO : 5. 聚合函数缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }
    // TODO : 6. 更新缓冲区数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0, buffer.getLong(0) + input.getLong(0))
      buffer.update(1, buffer.getLong(1) + 1L)
    }
    // TODO : 7. 合并缓冲区数据
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
      buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
    }
    // TODO : 8. 计算最终结果
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }
  }

}
