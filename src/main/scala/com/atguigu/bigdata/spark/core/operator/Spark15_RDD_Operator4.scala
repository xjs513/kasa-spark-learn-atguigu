package com.atguigu.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark15_RDD_Operator4 {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("Spark15_RDD_Operator4")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 4, 3, 2, 5, 6), 3)

    // TODO : mapPartitions
    // 以分区为单位进行计算，类似于批处理，和 map 算子类似
    // map 算子是全量数据操作，不允许丢失数据
    // mapPartitions 一次获取并处理分区的全部数据，可以进行迭代器集合的所有操作
    // mapPartitions 参数和返回值都必须是迭代器 Iterator[T]
    // mapPartitions 的问题: 容易 OOM
    // 如果分区数据没有处理完，那么所有数据都不会释放，包括已经处理的数据
    // 内存资源足够时，推荐使用 mapPartitions 提高效率
    // 否则使用 map 算子处理，完成比完美更重要!!~~!!

    // TODO 获取每个分区的最大值并标明是哪个分区
//    val rdd: RDD[String] = dataRDD.mapPartitionsWithIndex(
//      (index, iter) => {
//        List("分区[" + index + "]的最大元素是: " + iter.max).iterator
//      }
//    )

    // TODO 获取第二个分区的数据 索引从 0 开始
//    val resultRDD = dataRDD.mapPartitionsWithIndex(
//      (index, iter) => {
//        iter.map(ele => (index, ele)).filter(a => a._1 == 1)
//      }
//
    val resultRDD = dataRDD.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1){
          iter
        } else {
          Nil.iterator
        }
      }
    )




    // println(rdd.collect.mkString(","))
    resultRDD.collect.foreach(println)

    sc.stop()
  }
}
