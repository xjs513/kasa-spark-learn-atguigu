package com.atguigu.bigdata.spark.core.operator.singleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapTest {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("MapTest")
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
