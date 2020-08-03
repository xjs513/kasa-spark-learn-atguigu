package com.atguigu.bigdata.spark.core.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BroadcastDemoPre {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("BroadcastDemoPre")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // problem: 如果一个 Executor 中有多个任务的时候，会有大量的冗余数据，应该用 广播变量 broadcast
    val map: Map[String, Int] = Map("Lily" -> 24, "Lilei" -> 20)

    val rdd: RDD[String] = sc.makeRDD(List("Lily", "Lilei", "Lucy"))

    val result: RDD[(String, Int)] = rdd.map(name => (name, map.getOrElse(name, -1)))

    result.foreach(println)

    sc.stop()
  }
}
