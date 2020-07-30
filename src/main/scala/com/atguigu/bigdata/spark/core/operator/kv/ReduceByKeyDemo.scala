package com.atguigu.bigdata.spark.core.operator.kv

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object ReduceByKeyDemo {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("ReduceByKeyDemo")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // TODO : reduceByKey  集根据 key 分组和聚合于一起的算子

    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("hello", 1), ("scala", 1), ("hello", 1)
      )
    )

    val reduceByKeyRDD: RDD[(String, Int)] = rdd.reduceByKey(_ + _)

    reduceByKeyRDD.foreach(println)
    sc.stop()
  }
}
