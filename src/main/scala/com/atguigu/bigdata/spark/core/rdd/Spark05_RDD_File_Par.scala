package com.atguigu.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_File_Par {
  def main(args: Array[String]): Unit = {
    // TODO : Spark 创建运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_RDD_File")
    val sc: SparkContext = new SparkContext(conf)
    // TODO Spark 从File（磁盘文件）创建 RDD
    // TODO textFile 第一个参数表示读取文件的路径
    // TODO textFile 第二个参数表示最小分区的数量
    //               默认 = math.min(defaultParallelism, 2)

//    val fileRdd1: RDD[String] = sc.textFile("data\\input\\spark_01\\w.txt", 1)
//    println("fileRdd1 partitions:" + fileRdd1.partitions.length)
//    fileRdd1.saveAsTextFile("data\\output\\fileRdd1")
//
//    val fileRdd2: RDD[String] = sc.textFile("data\\input\\spark_01\\w.txt", 2)
//    println("fileRdd2 partitions:" + fileRdd2.partitions.length)
//    fileRdd2.saveAsTextFile("data\\output\\fileRdd2")
//
//    val fileRdd3: RDD[String] = sc.textFile("data\\input\\spark_01\\w.txt", 3)
//    println("fileRdd3 partitions:" + fileRdd3.partitions.length)
//    fileRdd3.saveAsTextFile("data\\output\\fileRdd3")
//
//    val fileRdd4: RDD[String] = sc.textFile("data\\input\\spark_01\\w.txt", 4)
//    println("fileRdd4 partitions:" + fileRdd4.partitions.length)
//    fileRdd4.saveAsTextFile("data\\output\\fileRdd4")

    val lines: RDD[String] = sc.textFile("data\\input\\spark_01\\line.txt")
    println("fileRdd4 partitions:" + lines.partitions.length)
    lines.saveAsTextFile("data\\output\\lines")

    sc.stop()
  }

  /**
    * TODO Scala
    * 1. math.min(a, b)
    * 2. math.max(a, b)
    *
    * TODO 从源码理解上述文件分片和分区之间的联系和区别
    * 1. Spark 读取文件默认采用 Hadoop 读取文件的规则
    *    1.1 文件切片规则： 按照字节数切片
    *    1.2 数据读取规则： 按找数据行读取
    * 2. TODO 问题：文件到底切成几片(分区数量)??
    *    回车换行占两个字节
    *    length = 文件字节数 = 10
    *    splitNum = 预计的切片数量
    *    length/splitNum = 10/3 = 3 余 1
    *    目标切片大小是3字节，多余1字节多余1字节
    *    如果多余字节数大于目标切片大小的十分之一，会额外多一个分片存放多余字节
    *    否则多余字节合并到最后一个分片中
    *    因此实际分区数可能大于最小分区数（minPartitions）
    *
    *    TODO 问题：分区的数据如何存储??
    *    分区数据读取规则： 按找数据行读取，而不是字节
    *
    * 3. FIXME : 研究 Hadoop 文件分片读取
    * 4.
    * 5.
    */
}
