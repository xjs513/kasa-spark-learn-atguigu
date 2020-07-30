package com.atguigu.bigdata.spark.core.operator.singleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsDemo {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("MapPartitionsDemo")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // TODO : mapPartitions
    // 以分区为单位进行计算，类似于批处理，和 map 算子类似
    // map 算子是全量数据操作，不允许丢失数据
    // mapPartitions 一次获取并处理分区的全部数据，可以进行迭代器集合的所有操作
    // mapPartitions 参数和返回值都必须是迭代器 Iterator[T]
    // mapPartitions 的问题: 容易 OOM
    // 如果分区数据没有处理完，那么所有数据都不会释放，包括已经处理的数据
    // 内存资源足够时，推荐使用 mapPartitions 提高效率
    // 否则使用 map 算子处理，完成比完美更重要!!~~!!

    val rdd: RDD[Int] = dataRDD.mapPartitions(iter => {
      // iter.map(_ * 2)

      // iter.filter(_ % 2 ==0)

      // iter.foreach(println)
      // iter

      List(iter.length).iterator
    })
    println(rdd.collect.mkString(","))

    sc.stop()
  }
}
