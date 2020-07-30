package com.atguigu.bigdata.spark.core.operator.singleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapDemo1 {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("MapDemo1")
      .setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    // println(rdd.partitions.length)
    // TODO Spark RDD 算子(方法)
    // 将旧的 RDD 通过算子转换为新的 RDD，但是不会触发作业的执行
    val rdd1: RDD[Int] = rdd.map(_ * 2)

    rdd1.foreach(println)

    sc.stop()
  }
}
