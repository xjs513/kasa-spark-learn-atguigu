package com.atguigu.bigdata.spark.core.operator.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CogroupDemo {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("CogroupDemo")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // TODO : cogroup

    val rdd1: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 1), ("b", 2), ("c", 3), ("a", 11), ("x", 110)
      )
    )

    val rdd2: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("c", 5), ("b", 6), ("a", 7), ("b", 16), ("y", 111)
      )
    )

    val resultRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)

    resultRDD.foreach(println)

    sc.stop()
  }
}
