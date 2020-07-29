package com.atguigu.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Operator1 {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("Spark10_RDD_Operator1")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // TODO 分区问题
    // RDD 中有分区列表 默认分区数不变，数据会转换后输出
    // TODO 分区之间的数据执行没有顺序，彼此之间无需等待
    // TODO 分区内元素按照顺序逐个参与转换，第一条数据执行完全部逻辑后才会执行下一条数据
    // 将旧的 RDD 通过算子转换为新的 RDD，但是不会触发作业的执行
    val rdd1: RDD[Int] = rdd.map(x => {
      println("map A ===>" + x)
      x
    })

    val rdd2: RDD[Int] = rdd1.map(x => {
      println("map B ===>" + x)
      x
    })

    println(rdd2.collect().mkString(","))

    sc.stop()
  }
}
