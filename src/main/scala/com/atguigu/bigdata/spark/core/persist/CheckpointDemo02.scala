package com.atguigu.bigdata.spark.core.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CheckpointDemo02 {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("CheckpointDemo02")
      .setMaster("local[*]")


    val sc: SparkContext = new SparkContext(conf)
    sc.setCheckpointDir("checkpoint\\PersistDemo03")

    //TODO 创建一个RDD new ParallelCollectionRDD
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4))

    val mapRDD: RDD[(Int, Int)] = rdd.map(num => {
      (num, 1)
    })

    // TODO: checkpoint 会切断血缘关系，等于产生新的数据源，如果数据丢失不会重新读取数据
    // TODO: 因为检查点会把数据保存到分布式文件系统，数据安全性高，不易丢失
    mapRDD.checkpoint()
    println(mapRDD.toDebugString)
    println(mapRDD.collect().mkString(", ")) // 8 map
    println(mapRDD.toDebugString)

    sc.stop()
  }
}
/**
(4) MapPartitionsRDD[1] at map at CheckpointDemo02.scala:20 []
 |  ParallelCollectionRDD[0] at makeRDD at CheckpointDemo02.scala:18 []
**************************************************************************
(4) MapPartitionsRDD[1] at map at CheckpointDemo02.scala:20 []
 |  ReliableCheckpointRDD[2] at collect at CheckpointDemo02.scala:27 []
*/