package com.atguigu.bigdata.spark.core.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Dep_Demo2 {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("Spark_Dep_Demo2")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //TODO 3.创建一个RDD new ParallelCollectionRDD
    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))
    println(rdd.dependencies)
    println("-------------------------------------------------")

    // TODO : new MapPartitionsRDD
    val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))
    println(flatMapRDD.dependencies)
    println("-------------------------------------------------")

    // TODO : new MapPartitionsRDD => new MapPartitionsRDD
    val mapRDD: RDD[(String, Int)] = flatMapRDD.map((_, 1))
    println(mapRDD.dependencies)
    println("-------------------------------------------------")

    // TODO : new ShuffledRDD[K, V, C]
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    println(reduceRDD.dependencies)


    println(reduceRDD.collect().mkString(", "))

    sc.stop()
  }
}

/**
List()
-------------------------------------------------
List(org.apache.spark.OneToOneDependency@6ac97b84)
-------------------------------------------------
List(org.apache.spark.OneToOneDependency@6579c3d9)
-------------------------------------------------
List(org.apache.spark.ShuffleDependency@69da0b12)

collect() => runJOb => dagScheduler.runJob => submitJob => JobSubmitted =>

handleJobSubmitted[阶段的划分：createResultStage 包括：getOrCreateParentStages]
*/