package com.atguigu.bigdata.spark.core.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_RDD_File")
    val sc: SparkContext = new SparkContext(conf)
    // todo : Spark 从File（磁盘文件）创建 RDD
    // todo : 1.textFile(path:String) 并行
    // todo : path 可以是文件或目录, 可以设置相对路径, 可以用正则表达式匹配
    // val fileRdd: RDD[String] = sc.textFile("data\\input\\spark_01\\a.txt")
    // val fileRdd: RDD[String] = sc.textFile("data\\input\\spark_01")
    // val fileRdd: RDD[String] = sc.textFile("data\\input\\spark_01\\*.txt")
    // 在 IDEA 中 相对路径从项目的根目录开始查找
    // path 根据环境的不同而自动改变
    // path 还可以是第三方存储系统： hdfs

    // Spark 读取文件默认采用 Hadoop 读取文件的规则
    // 逐行读取
    val fileRdd: RDD[String] = sc.textFile("data\\input\\spark_01\\*.txt")

    println(fileRdd.partitions.length)

    fileRdd.foreach(println)

    // fileRdd.saveAsTextFile("data\\output\\Spark03_RDD_Memory_Par")

    sc.stop()
  }
}
