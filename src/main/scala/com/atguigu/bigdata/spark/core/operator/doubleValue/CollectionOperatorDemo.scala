package com.atguigu.bigdata.spark.core.operator.doubleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CollectionOperatorDemo {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("MapTest")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // TODO : 双 RDD 的集合操作 注意分区如何变化 要求泛型一致才行(zip例外)

    val rdd1 = sc.makeRDD(List(1, 2, 3, 4), 3)
    val rdd2 = sc.makeRDD(List(3, 4, 5, 6), 2)

    // TODO : 并集 分区数为两者分区数之和  没有洗牌
    val rdd3: RDD[Int] = rdd1.union(rdd2)
    // println(rdd3.collect.mkString(", "))
    rdd3.saveAsTextFile("data\\output\\rdd3")

    // TODO : 交集 分区数为两者中最大值 数据打乱重组 有洗牌过程
    val rdd4: RDD[Int] = rdd1.intersection(rdd2)
    // println(rdd4.collect.mkString(", "))
    rdd4.saveAsTextFile("data\\output\\rdd4")

    // TODO : 差集 分区数为当前RDD分区数 数据打乱重组 有洗牌过程
    val rdd5: RDD[Int] = rdd1.subtract(rdd2)
    // println(rdd5.collect.mkString(", "))
    rdd5.saveAsTextFile("data\\output\\rdd5")

    // todo: 要求严格：拉链 分区数和分区内元素数都一致才能进行拉链操作
    // TODO : 1. 分区数一样，分区内元素数不一样
    // Exception: Can only zip RDDs with same number of elements in each partition
    // TODO : 2. 分区数不一样，分区内元素一样，总元素数不一样
    // Exception: Can't zip RDDs with unequal number of partitions: List(3, 2)

     val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
     println(rdd6.collect.mkString(", "))
     rdd6.saveAsTextFile("data\\output\\rdd6")

    sc.stop()
  }
}
