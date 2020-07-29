package com.atguigu.bigdata.spark.core.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark07_RDD_File_PartitionData1 {
  def main(args: Array[String]): Unit = {
    // TODO : Spark 创建运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_RDD_File")
    val sc: SparkContext = new SparkContext(conf)
    // TODO Spark 从File（磁盘文件）创建 RDD
    // TODO 分区数量
    /**
      * 文件大小 10
      * 预计分片 4
      * 片大小 2 剩余 2 2大于2的十分之一
      * 所以分片数量为 4 + 1 = 5
      *
      * 1@@ => 012
      * 2@@ => 345
      * 3@@ => 678
      * 4   => 9
      *
      * 0 => (0, 0+2) => (0, 2) 3个字节[102] 1@@
      * 1 => (2, 2+2) => (2, 4) 3个字节[2忽略 34正常读取 5同行顺延] 2@@
      * 2 => (4, 4+2) => (4, 6) 3个字节[45忽略 6正常读取 78同行顺延] 3@@
      * 3 => (6, 6+2) => (6, 8) 3个字节[678忽略] 空值
      * 4 => (8, 8+2) => (8, 10)1个字节[8忽略 9正常读取 结束] 4
      */
    // TODO 分区数据读取
    // 数据按行读取，但同时考虑偏移量(offset)
    // *** 区间都时闭区间，之前被读走的忽略不计，同一行向后顺延 ***

    // TODO 1. 分片数 2. 整行读取  3. 基于偏移量

    val lines: RDD[String] = sc.textFile("data\\input\\spark_01\\w.txt", 4)
    println("fileRdd4 partitions:" + lines.partitions.length)
    lines.saveAsTextFile("data\\output\\lines")

    sc.stop()
  }
}
