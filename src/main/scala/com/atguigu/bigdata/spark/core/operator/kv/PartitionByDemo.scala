package com.atguigu.bigdata.spark.core.operator.kv

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object PartitionByDemo {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("MapTest")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // TODO : KV 转换算子 partitionBy
    // TODO : 根据给定的 Partitioner 重新分区
    // TODO : 默认 Partitioner 是 HashPartitioner

    // todo : groupBy coalesce repartition

    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    // 先转换成 PairRDD 才能用 partitionBy 算子
    // PairRDDFunctions 隐式转换
    // RDD 的半生对象中提供了隐式转换函数 RDD[K, V] =>  PairRDDFunctions[K, V]
    val keyValueRDD: RDD[(Int, Int)] = rdd.map(a => (a, a))

    println(keyValueRDD.partitions.length)

    // TODO : partitionBy 参数为分区器对象： HashPartitioner & RangePartitioner

    // TODO : spark 默认分区器就是 HashPartitioner 分区规则是将当前数据的 key.hashCode 进行取余

    // TODO : sortBy 使用了 RangePartitioner 要求 key 必须能够排序

    val value: RDD[(Int, Int)] = keyValueRDD.partitionBy(new HashPartitioner(2))
    println(value.partitions.length)

    sc.stop()
  }
}
