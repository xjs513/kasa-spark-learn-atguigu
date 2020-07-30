package com.atguigu.bigdata.spark.core.operator.singleValue

import org.apache.spark.{SparkConf, SparkContext}

object SortByDemo {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("MapTest")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val dataRDD = sc.makeRDD(List(1, 4, 2, 3))

    // val sortRDD = dataRDD.sortBy(a=>a)

    val sortRDD = dataRDD.sortBy(a=>a, false)

    println(sortRDD.collect.mkString(", "))


    sortRDD.saveAsTextFile("data\\output\\SortByDemo")

    sc.stop()
  }
}
