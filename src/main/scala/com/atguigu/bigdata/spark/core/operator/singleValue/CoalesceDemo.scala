package com.atguigu.bigdata.spark.core.operator.singleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CoalesceDemo {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("CoalesceDemo")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 1, 1, 2, 2, 2), 2)

    val filterRDD = dataRDD.filter(_%2 ==0)

    // TODO : coalesce  缩减分区数
    // TODO : 数据过滤后发现严重不均匀  或者每个分区数据很少 数据分布不合理
    // 数据没有过滤也能缩减分区  分区并不是越多越好 也不是分区越多任务完成越快
    val coalesceRDD = filterRDD.coalesce(1)


    coalesceRDD.saveAsTextFile("data\\output\\coalesceRDD")

    sc.stop()
  }
}
