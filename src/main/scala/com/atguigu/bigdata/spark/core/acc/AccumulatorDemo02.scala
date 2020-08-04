package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorDemo02 {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("AccumulatorDemo02")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val sum: LongAccumulator = sc.longAccumulator("sum")

    //TODO 3.创建一个RDD new ParallelCollectionRDD
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4))

    rdd.foreach{
      case i:Int => {
        sum.add(i)
      }
    }

    println("sum = " + sum.value)

    sc.stop()
  }
}
