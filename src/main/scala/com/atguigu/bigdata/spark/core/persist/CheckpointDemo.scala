package com.atguigu.bigdata.spark.core.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CheckpointDemo {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("CheckpointDemo")
      .setMaster("local[*]")


    val sc: SparkContext = new SparkContext(conf)
    sc.setCheckpointDir("checkpoint\\PersistDemo03")

    //TODO 创建一个RDD new ParallelCollectionRDD
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4))

    val mapRDD: RDD[(Int, Int)] = rdd.map(num => {
      println("map......")
      (num, 1)
    })

    // TODO : 将比较重要、计算耗时的数据保存到分布式文件系统中 checkpoint

    // 检查点为了保证数据的准确性，会启动单独的Job
    // 为了提高性能，一般会和缓存一起使用

    mapRDD.checkpoint()

    println(mapRDD.collect().mkString(", ")) // 8 map
    println("*********************************************************************")
    sc.stop()
  }
}