package com.atguigu.bigdata.spark.core.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PersistDemo02 {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("PersistDemo02")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //TODO 创建一个RDD new ParallelCollectionRDD
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4))

    val mapRDD: RDD[(Int, Int)] = rdd.map(num => {
      println("map......")
      (num, 1)
    })

    // TODO : cache 操作在行动算子执行后，会在血缘中添加缓存相关的依赖
    // 也就是说行动(任务触发)之后才起作用
    val cacheRDD: RDD[(Int, Int)] = mapRDD.cache()
    // val cacheRDD: RDD[(Int, Int)] = mapRDD.persist(StorageLevel.DISK_ONLY_2)

    println(cacheRDD.toDebugString)

    // TODO collect
    println(cacheRDD.collect().mkString(", "))

    println(cacheRDD.toDebugString)


    sc.stop()
  }
}

/**
map......
  map......
  map......
  map......
  (1,1), (2,1), (3,1), (4,1)
  map......
  map......
  map......
  map.....
  map 算子会执行两遍，因为 RDD 不存储数据，每次都要重新计算
  为了提高效率，避免重复计算，可以用缓存, 重复使用数据 mapRDD.cache()。
******************************************************
  map......
  map......
  map......
  map......
  (1,1), (2,1), (3,1), (4,1)
*/