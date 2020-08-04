package com.atguigu.bigdata.spark.core.operator.kv

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CombineByKeyDemo {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("CombineByKeyDemo")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // TODO : combineByKey  集根据 key 分组和聚合于一起的算子

    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 1), ("a", 2), ("c", 3),
        ("b", 4), ("c", 5), ("c", 6)
      ), 2
    )
    // TODO 将分区内相同 key 取最大值，分区间相同的 key 求和
    // 分区内和分区间计算规则不一样
    // reduceByKey = 分区内和分区间计算规则一样

    // TODO : combineByKey
    // 参数列表中的参数为:
    //      createCombiner: V => C,       表示值的转换结构
    //      mergeValue: (C, V) => C,      表示分区内的计算规则
    //      mergeCombiners: (C, C) => C,  表示分区间的计算规则

    // TODO : 求相同 key 的平均值
    val combineRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
      (v: Int) => (v, 1),
      (c: (Int, Int), v: Int) => (c._1 + v, c._2 + 1),
      (c1: (Int, Int), c2: (Int, Int)) => (c1._1 + c2._1, c1._2 + c2._2)
    )
//    rdd.combineByKey(
//      v => (v, 1),
//      (c:(Int,Int), v) => (c._1+v, c._2+1),
//      (c1:(Int,Int), c2:(Int,Int)) => (c1._1+c2._1,c1._2+c2._2)
//    )

    val resultRDD: RDD[(String, Int)] = combineRDD.map(t3 => (t3._1, t3._2._1/t3._2._2))

    resultRDD.collect().foreach(println)
    println(resultRDD.partitions.length)

    sc.stop()
  }
}
