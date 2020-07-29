package com.atguigu.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Test {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("Spark11_RDD_Test")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // TODO 从服务器日志数据 apache.log 中获取用户请求的 URL 资源路径
    val fileRDD: RDD[String] = sc.textFile("data\\input\\apache.log")

    val urlRDD: RDD[String] = fileRDD.map(line => {
      line.split(" ")(6)
    })
    urlRDD.collect().foreach(println)

    sc.stop()
  }
}
