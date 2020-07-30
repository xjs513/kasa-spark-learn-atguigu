package com.atguigu.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FlatMapDemo {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("FlatMapDemo")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val dataRDD: RDD[List[Int]] = sc.makeRDD(List(
      List(1, 2), List(3, 4)
      //1, 2
    ))
    // TODO : flatMap
    // 输入和输出一样的时候 不能用 _ 编译器有歧义
    val rdd: RDD[Int] = dataRDD.flatMap(list => list)

     println(rdd.collect.mkString(","))
    // resultRDD.collect.foreach(println)

    sc.stop()
  }
}
