package com.atguigu.bigdata.spark.core.operator.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FoldByKeyDemo {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("FoldByKeyDemo")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // TODO : foldByKey  集根据 key 分组和聚合于一起的算子

    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 1), ("a", 2), ("c", 3),
        ("b", 4), ("c", 5), ("c", 6)
      ), 2
    )
    // TODO 将分区内相同 key 取最大值，分区间相同的 key 求和
    // 分区内和分区间计算规则不一样
    // reduceByKey = 分区内和分区间计算规则一样

    // TODO : aggregateByKey => foldByKey
    // Scala 语法: 函数柯里化
    // 两套参数列表需要传递参数
    // 第一个参数列表中的参数为: zeroValue: U 计算的初始值  初始值参与分区内第一个出现的数据的运算
    // 第二个参数列表中的参数为: (seqOp: (U, V) => U,combOp: (U, U) => U)
    // seqOp: (U, V) => U  表示分区内的计算规则 相同的Value如何计算
    // combOp: (U, U) => U 表示分区间的计算规则 相同的Value如何计算
    // TODO : 当 seqOp = combOp 的时候 aggregateByKey 退化为 foldByKey

    val resultRDD: RDD[(String, Int)] = rdd.foldByKey(0)( _ + _)
    resultRDD.collect().foreach(println)
    println(resultRDD.partitions.length)

    sc.stop()
  }
}
