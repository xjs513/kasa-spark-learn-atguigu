package com.atguigu.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_RDD_Operator6 {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("Spark19_RDD_Operator6")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // TODO : glom
    // 将分区数据转换为数组
    val rdd: RDD[Array[Int]] = dataRDD.glom()

    //     println(rdd.collect.mkString(","))
    //    rdd.collect.foreach(arr => println(arr.mkString(",")))

    // TODO : 计算所有分区的最大值求和，分区内取最大值，分区间最大值求和
    val maxPerPartition: RDD[Int] = rdd.map(_.max)
    val sum: Int = maxPerPartition.reduce(_ + _)
    println("sum = " + sum)

    sc.stop()
  }
}
