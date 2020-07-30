package com.atguigu.bigdata.spark.core.operator.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupByKeyDemo {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("GroupByKeyDemo")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // TODO : groupByKey  根据 key 分组算子
    // TODO : 面向整个数据集，而不是某个分区

    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("hello", 1), ("scala", 1), ("hello", 1)
      )
    )

    val groupByKeyRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()

//    val resultRDD: RDD[(String, Int)] = groupByKeyRDD.map {
//      case (word, iter) => {
//        (word, iter.sum)
//      }
//    }

    val resultRDD: RDD[(String, Int)] = groupByKeyRDD.map((x:(String, Iterable[Int])) => (x._1, x._2.sum))

    resultRDD.foreach(println)
    sc.stop()
  }
}
