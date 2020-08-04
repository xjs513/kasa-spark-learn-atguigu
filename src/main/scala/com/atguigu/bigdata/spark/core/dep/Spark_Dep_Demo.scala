package com.atguigu.bigdata.spark.core.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Dep_Demo {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("Spark_Dep_Demo")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //TODO 3.创建一个RDD new ParallelCollectionRDD
    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))
    println(rdd.toDebugString)
    println("-------------------------------------------------")

    // TODO : new MapPartitionsRDD
    val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))
    println(flatMapRDD.toDebugString)
    println("-------------------------------------------------")

    // TODO : new MapPartitionsRDD => new MapPartitionsRDD
    val mapRDD: RDD[(String, Int)] = flatMapRDD.map((_, 1))
    println(mapRDD.toDebugString)
    println("-------------------------------------------------")

    // TODO : new ShuffledRDD[K, V, C]
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    println(reduceRDD.toDebugString)


    println(reduceRDD.collect().mkString(", "))

    sc.stop()
  }
}

/**
(4: 这个是分区数) ParallelCollectionRDD[0] at makeRDD at Spark_Dep_Demo.scala:15 []
-------------------------------------------------
(4) MapPartitionsRDD[1] at flatMap at Spark_Dep_Demo.scala:20 []
 |  ParallelCollectionRDD[0] at makeRDD at Spark_Dep_Demo.scala:15 []
-------------------------------------------------
(4) MapPartitionsRDD[2] at map at Spark_Dep_Demo.scala:25 []
 |  MapPartitionsRDD[1] at flatMap at Spark_Dep_Demo.scala:20 []
 |  ParallelCollectionRDD[0] at makeRDD at Spark_Dep_Demo.scala:15 []
-------------------------------------------------
(4) ShuffledRDD[3] at reduceByKey at Spark_Dep_Demo.scala:30 []
 +-(4) MapPartitionsRDD[2] at map at Spark_Dep_Demo.scala:25 []
    |  MapPartitionsRDD[1] at flatMap at Spark_Dep_Demo.scala:20 []
    |  ParallelCollectionRDD[0] at makeRDD at Spark_Dep_Demo.scala:15 []
*/