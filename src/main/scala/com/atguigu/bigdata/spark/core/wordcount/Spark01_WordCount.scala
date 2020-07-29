package com.atguigu.bigdata.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_WordCount")
    val sc: SparkContext = new SparkContext(conf)

    val fileRdd: RDD[String] = sc.textFile("data\\input\\spark_01")

    val wordRDD: RDD[String] = fileRdd.flatMap(_.split(" "))


    val groupRDD: RDD[(String, Iterable[String])] = wordRDD.groupBy(a => a)

    // val wordCountRDD: RDD[(String, Int)] = groupRDD.mapValues(_.size)
    // OR use next map method to finish
    val wordCountRDD: RDD[(String, Int)] = groupRDD.map{
      case (word, iterable) => {
        (word, iterable.size)
      }
    }

    val tuples: Array[(String, Int)] = wordCountRDD.collect()

    println(tuples.mkString(", "))

    sc.stop()

  }
}
