package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object AccumulatorDemo {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("AccumulatorDemo")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //TODO 3.创建一个RDD new ParallelCollectionRDD
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4))

    val sum: Int = rdd.reduce(_ + _)

    println("sum = " + sum)

    sc.stop()
  }
}
