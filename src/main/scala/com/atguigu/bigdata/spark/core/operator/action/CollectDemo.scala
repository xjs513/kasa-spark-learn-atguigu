package com.atguigu.bigdata.spark.core.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CollectDemo {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("CollectDemo")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // TODO : cogroup

//    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val rdd: RDD[User] = sc.makeRDD(List(
      new User("张三", 17),
      new User("李四", 18),
      new User("王五", 19)
    ))


    val resultRDD: RDD[(User, Int)] = rdd.map(u => (u, 123))


    val result: Array[(User, Int)] = resultRDD.collect()

    println(result.mkString(", "))

    sc.stop()
  }
}


class User(name:String, age:Int) extends Serializable {

  override def toString: String = {
    "name = [" + name + "], age [" + age + "]"
  }
}