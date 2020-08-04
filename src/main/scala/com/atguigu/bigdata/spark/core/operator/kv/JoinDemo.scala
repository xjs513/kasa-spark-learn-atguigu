package com.atguigu.bigdata.spark.core.operator.kv

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object JoinDemo {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("JoinDemo")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // TODO : join rightOuterJoin leftOuterJoin

    val rdd1: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 1), ("b", 2), ("c", 3)
      )
    )

    val rdd2: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("c", 5), ("b", 6), ("a", 7)
      )
    )

    val resultRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
//    val resultRDD: RDD[(String, (Option[Int], Int))] = rdd1.rightOuterJoin(rdd2)
//    val resultRDD: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)

    resultRDD.foreach(println)

    sc.stop()
  }
}
