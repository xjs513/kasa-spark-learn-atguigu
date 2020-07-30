package com.atguigu.bigdata.spark.core.operator.kv

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object PartitionerDemo2 {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("PartitionerDemo2")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // TODO : 自定义分区器
    // cba, wnba, nba

    val rdd: RDD[(String, String)] = sc.makeRDD(
      List(
        ("cba", "消息1"), ("cba", "消息2"), ("cba", "消息3"),
        ("nba", "消息4"), ("wnba", "消息5"), ("nba", "消息6")
      )
    )

    // todo : 如果分区器和当前的一样，不会重分区  阅读源码：先比分区器类型 然后比分区数
    val rdd1: RDD[(String, String)] = rdd.partitionBy(new HashPartitioner(3))
    val rdd2: RDD[(String, String)] = rdd1.partitionBy(new HashPartitioner(3))

    println(rdd2.partitions.length)
    rdd2.collect.foreach(println)

    sc.stop()
  }
}