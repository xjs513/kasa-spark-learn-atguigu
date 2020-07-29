package com.atguigu.bigdata.spark.core.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark04_RDD_Memory_PartitionData {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark03_RDD_Memory_PartitionData")
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
    //val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // 将RDD的数据保存到分区文件中
    // todo : 12 + 34 YES  内存中的集合数据按照平均分割进行分区处理
    // todo : 13 + 24 NO   不是轮询分配
    //rdd.saveAsTextFile("data\\output\\rdd")

    //val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 4)
    // todo : 1 + 2 + 3 + 4  这个容易理解
    //rdd1.saveAsTextFile("data\\output\\rdd1")

    //val rdd2: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 3)
    // todo : 14 + 2 + 3 NO
    // todo : 12 + 3 + 4 NO
    // todo : 1 + 2 + 34 YES 这个如何理解??
    //rdd2.saveAsTextFile("data\\output\\rdd2")

    val rdd3: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 3)
    // todo : 1 + 23 + 45 YES 这个又如何理解??
    rdd3.saveAsTextFile("data\\output\\rdd3")

    // TODO 从源码理解上述问题，顺便熟悉下 Scala 语法
    /**
      * 1. 泛型
      * 2. 方法的重写 override def getPartitions:Array[Partition] = {...}
      * 3. 半生对象 通过类名称直接访问方法
      * 4. 模式匹配 match {case ... } 匹配：常量、规则、集合、列表、类型、元组、对象 _ 的问题
      * 5. Array 的 slice（from:Int, util:Int） 方法
      */

    /** [0, 1, 2] ==> (0, 1) (1, 3) (3, 5)
     def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
       (0 until numSlices).iterator.map { i =>
         val start = ((i * length) / numSlices).toInt
         val end = (((i + 1) * length) / numSlices).toInt
         (start, end)
       }
     }
      */

    sc.stop()
  }
}
