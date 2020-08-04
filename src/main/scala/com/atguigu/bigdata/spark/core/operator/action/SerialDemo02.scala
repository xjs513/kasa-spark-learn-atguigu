package com.atguigu.bigdata.spark.core.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SerialDemo02 {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("SerialDemo02")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val user = new User
    rdd.foreach(
      (num: Int) => {
          println("age = " + (user.age + num))
        }
    )


    sc.stop()

  }

  // org.apache.spark.SparkException: Task not serializable
  // Caused by: java.io.NotSerializableException:
  // com.atguigu.bigdata.spark.core.operator.action.SerialDemo02$User
  // 如果算子使用了算子外的对象，那么需要对象能序列化
  // job 提交之前先检查序列化

//  class User {
//  class User extends Serializable { // 这样能正常运行
//    val age:Int = 20
//  }

  case class User(age:Int = 20)

}
