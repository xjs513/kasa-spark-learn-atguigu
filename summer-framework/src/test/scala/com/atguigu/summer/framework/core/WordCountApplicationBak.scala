//package com.atguigu.summer.framework.core
//
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//
//object WordCountApplicationBak extends TApplication {
//  def main(args: Array[String]): Unit = {
//    start("spark"){
//
//      val controller = new WordCountController
//      controller.execute()
//
//      val sc:SparkContext = envData.asInstanceOf[SparkContext]
//      val fileRDD = sc.textFile("data\\input\\spark_01\\a.txt")
//
//      val resultRDD: RDD[(String, Int)] = fileRDD.flatMap(_.split(" "))
//        .map((_, 1))
//        .reduceByKey(_ + _)
//
//      // println(resultRDD.collect().mkString(", "))
//
//      resultRDD.collect().foreach(println)
//
//    }
//  }
//}
