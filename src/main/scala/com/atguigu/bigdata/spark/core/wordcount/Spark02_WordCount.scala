package com.atguigu.bigdata.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_WordCount")
    val sc: SparkContext = new SparkContext(conf)

    val fileRdd: RDD[String] = sc.textFile("data\\input\\spark_01")

    val wordRDD: RDD[String] = fileRdd.flatMap(_.split(" "))

    val pairRDD: RDD[(String, Int)] = wordRDD.map((_, 1))

    val wordCountRDD: RDD[(String, Int)] = pairRDD.reduceByKey(_ + _)

    val tuples: Array[(String, Int)] = wordCountRDD.collect()

    println(tuples.mkString(", "))

    sc.stop()

  }
}
