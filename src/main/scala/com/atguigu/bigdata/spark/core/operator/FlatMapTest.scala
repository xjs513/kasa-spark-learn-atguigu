package com.atguigu.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FlatMapTest {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("FlatMapTest")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // TODO 扁平化处理 List(List(1, 2), 3, List(4, 5))

    val list: List[Any] = List(List(1, 2), 3, List(4, 5))

    val rdd: RDD[Any] = sc.makeRDD(list).flatMap(a => {
      a match {
        case seq: Seq[_] => seq
        case d => List(d)
      }
    })

    println(rdd.collect.mkString(","))

    sc.stop()
  }
}
