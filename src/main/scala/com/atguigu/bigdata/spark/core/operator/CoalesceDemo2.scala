package com.atguigu.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CoalesceDemo2 {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("CoalesceDemo2")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 1, 1, 2, 2, 2), 1)

    // TODO : coalesce  缩减分区数 提高小数据集执行效率 减少调度成本
    // TODO : 数据过滤后发现严重不均匀  或者每个分区数据很少 数据分布不合理
    // 数据没有过滤也能缩减分区  分区并不是越多越好 也不是分区越多任务完成越快
    // TODO : coalesce 默认不洗牌，不能扩大分区 连空的分区都不会出现 阅读源码可以理解
    // TODO : coalesce 要想扩大分区，必须洗牌才行

    // 也可以用 repartition 算子，其实就是调用 coalesce 算子， shuffle 默认设置为 true

//    val coalesceRDD = dataRDD.coalesce(2, false)

    val repartitionRDD = dataRDD.repartition(2)

    repartitionRDD.saveAsTextFile("data\\output\\repartitionRDD")

    sc.stop()
  }
}
