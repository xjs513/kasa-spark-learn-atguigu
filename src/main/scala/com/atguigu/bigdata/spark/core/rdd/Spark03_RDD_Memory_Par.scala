package com.atguigu.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark03_RDD_Memory_Par")
    val sc: SparkContext = new SparkContext(conf)
    // todo : Spark 从内存中创建 RDD
    // TODO : RDD 中分区数量就是并行度，设置并行度也就是在设置分区数量
    // todo : 1.parallelize 并行
    // todo : 2.makeRDD 实际也是调用了 parallelize

    // todo : 第一个参数：数据源
    // todo : 第二个参数：并行度(分区数量) 当前的计算任务能同时进行多少个
    // todo : numSlices: Int = defaultParallelism  默认并行度

    // scheduler.conf.getInt("spark.default.parallelism", totalCores)
    // 默认并行度从 conf 中获取，如果没有配置，则用默认值 totalCores (机器的总核数)
    // 机器的总核数 = 当前环境可用核数
    /**
      * local  numSlices = 3  => 3
      * local[*] => 4
      * local[7] => 7
      *
      * numSlices 的优先级更高
      */

    // todo: 分区的数量一定就是并行度么？？
    /**
      * 当然不一定, 资源有限的情况下, 三个分区, 一个 CPU
      * 这时并行就没有了, 变成并发
      */

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 3)

     // println(rdd.partitions.length)

    // println(rdd.collect().mkString(","))

    // 将RDD的数据保存到分区文件中
    rdd.saveAsTextFile("data\\output\\Spark03_RDD_Memory_Par")


    sc.stop()
  }
}
