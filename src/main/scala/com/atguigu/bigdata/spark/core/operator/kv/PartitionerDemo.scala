package com.atguigu.bigdata.spark.core.operator.kv

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object PartitionerDemo {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("MapTest")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // TODO : 自定义分区器
    // cba, wnba, nba

    val rdd: RDD[(String, String)] = sc.makeRDD(
      List(
        ("cba", "消息1"), ("cba", "消息2"), ("cba", "消息3"),
        ("nba", "消息4"), ("wnba", "消息5"), ("nba", "消息6")
      )
    )
    val rdd1: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner(3))


    val rdd2: RDD[(Int, (String, String))] = rdd1.mapPartitionsWithIndex(
      (index, data) => {
        data.map(
          ele => (index, ele)
        )
      }
    )
    rdd2.filter(_._1==1).foreach(println)

    sc.stop()
  }

}

// TODO : 自定义分区器
// 1. 继承抽象类 Partitioner
// 2. 重写抽象方法
class MyPartitioner(num:Int) extends Partitioner{

  // 获取分区数量
  override def numPartitions: Int = num

  // 根据key决定数据归属的分区编号
  override def getPartition(key: Any): Int = {
    key match {
      case "nba" => 0
      case   _   => 1
    }
  }
}