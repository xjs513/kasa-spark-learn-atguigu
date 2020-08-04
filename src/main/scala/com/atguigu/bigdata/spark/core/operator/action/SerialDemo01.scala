package com.atguigu.bigdata.spark.core.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SerialDemo01 {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("SerialDemo01")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    rdd.foreach(
      (num: Int) => {
        val user = new User
        println("age = " + (user.age + num))
      }
    )
    /**
    * age = 24
    * age = 21
    * age = 22
    * age = 23
    */
    sc.stop()
  }

  class User {
    val age:Int = 20
  }
}
