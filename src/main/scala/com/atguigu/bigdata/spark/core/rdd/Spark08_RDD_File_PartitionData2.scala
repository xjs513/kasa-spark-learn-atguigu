package com.atguigu.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_File_PartitionData2 {
  def main(args: Array[String]): Unit = {
    // TODO : Spark 创建运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_RDD_File")
    val sc: SparkContext = new SparkContext(conf)
    // TODO Spark 从File（磁盘文件）创建 RDD
    // TODO 分区数量
    /**
      * 文件大小 6
      * 预计分片 2
      * 片大小 3 剩余 0
      * 所以分片数量为 2
      *
      * 1@@ => 012
      * 234 => 345
      *
      * 0 => (0, 0+3) => (0, 3) 6个字节[0123正常读取 45同行顺延]
      * 1 => (3, 3+3) => (3, 6) 0
      */
    // TODO 分区数据读取
    // 数据按行读取，但同时考虑偏移量(offset)
    // *** 闭区间，之前被读走的忽略不计，同一行向后顺延 ***

    // TODO 1. 分片数 2. 整行读取  3. 基于偏移量

//    val lines: RDD[String] = sc.textFile("data\\input\\spark_01\\y.txt", 2)
//    println("fileRdd4 partitions:" + lines.partitions.length)
//    lines.saveAsTextFile("data\\output\\lines")

    // TODO 多文件
    val lines: RDD[String] = sc.textFile("data\\input\\multi", 3)
    println("fileRdd4 partitions:" + lines.partitions.length)
    lines.saveAsTextFile("data\\output\\multi")

    // TODO Hadoop 分片以文件为单位进行，数据读取不能跨文件
    // TODO 统计文件大小时包括所有文件
    // 12 / 2 = 6
    // (0, 6)
    // (0, 6)

    // 12 / 3 = 4
    // (0, 4) (4, 8)
    // (0, 4) (4, 8)

    // 18 / 3 = 6
    // (0, 6)
    // (0, 6) (6, 12)

    sc.stop()
  }
}
