package com.atguigu.bigdata.spark.core.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BroadcastDemo {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("BroadcastDemoPre")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val map: Map[String, Int] = Map("Lily" -> 24, "Lilei" -> 20)

    val broadcastMap: Broadcast[Map[String, Int]] = sc.broadcast(map)

    val rdd: RDD[String] = sc.makeRDD(List("Lily", "Lilei", "Lucy"))

    // 广播变量 : 分布式只读共享变量
    // 在 executor 的公共区域只有一份，没有冗余数据
    val result: RDD[(String, Int)] = rdd.map(name => (name, broadcastMap.value.getOrElse(name, -1)))

    result.foreach(println)

    sc.stop()
  }
}
