package com.atguigu.bigdata.spark.core.operator.kv

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SortByKeyDemo {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("SortByKeyDemo")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // TODO : sortByKey  根据 key 排序

    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 1), ("a", 2), ("c", 3),
        ("b", 4), ("c", 5), ("c", 6)
      ), 2
    )

    val sortRDD: RDD[(String, Int)] = rdd.sortByKey(true)

    val resultRDD: RDD[Iterator[(Int, String, Int)]] = sortRDD.mapPartitionsWithIndex(
      (index, iter) => {
        iter.map {
          case (str, int) => {
            List((index, str, int)).iterator
          }
        }
      }
    )
    resultRDD.foreach(
      iter => iter.foreach(println)
    )

//    println(resultRDD.collect.mkString(", "))

    sc.stop()

  }

}
