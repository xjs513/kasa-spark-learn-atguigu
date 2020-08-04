package com.atguigu.bigdata.spark.core.persist

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object PersistDemo01 {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("PersistDemo01")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //TODO 创建一个RDD new ParallelCollectionRDD
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4))

    val mapRDD: RDD[(Int, Int)] = rdd.map(num => {
      println("map......")
      (num, 1)
    })

    // 缓存计算结果 重复使用 提高效率。
    // 默认保存在 Executor 的内存中，数据量大的时候如何处理??
    // 源码调用了 persist() 采用不同的 StorageLevel 对数据进行持久化。
    // cache 存储数据在内存中，如果空间不够，Executor 对数据进行整理，然后丢弃数据。
    // 如果由于 Executor 端整理数据导致缓存的数据丢失，那么需要重新执行数据计算。
    // 如果 cache 后的数据需要重新计算，那么必须遵守血缘关系，所以 cache 操作不能切断血缘。
    val cacheRDD: RDD[(Int, Int)] = mapRDD.cache()

    // TODO collect
    println(cacheRDD.collect().mkString(", "))

    // TODO save
    cacheRDD.saveAsTextFile("data\\output\\persist01")


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