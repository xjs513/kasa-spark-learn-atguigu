package com.atguigu.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsTest {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("MapPartitionsTest")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // TODO 获取每个分区的最大值
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 4, 3, 2, 5, 6), 2)

    // mapPartitions 参数和返回值都必须是迭代器 Iterator[T]
    val resultRDD: RDD[Int] = dataRDD.mapPartitions(
      iter => {
        List(iter.max).iterator
      }
    )
    println(resultRDD.collect().mkString(","))

    sc.stop()
  }
}
