package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object AccumulatorDemo03 {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("AccumulatorDemo03")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // 1. 创建自定义累加器
    val acc = new MyWordCountAccumulator
    val accumulator: LongAccumulator = new LongAccumulator

    // 2. 注册自定义累加器
    sc.register(acc)
    sc.register(accumulator)

    //TODO 3.创建一个RDD new ParallelCollectionRDD
    val rdd: RDD[String] = sc.makeRDD(List("hello scala", "hello", "spark", "scala"))

    // 3. 使用累加器
    rdd.flatMap(_.split(" ")).foreach{
      word => {
        accumulator.add(1)
        acc.add(word)
      }
    }

    // 4. 获取累加器的值
    println("accumulator = " + accumulator.value)
    println("acc = " + acc.value)

    sc.stop()
  }

  // TODO : 自定义累加器
  /**
    * 1. 继承 AccumulatorV2，定义泛型 [IN, OUT]
    *    IN: 输入类型
    *    OUT:返回类型
    * 2. 重写累加器的抽象方法 6个方法
    **********************************************
    * 累加器方法调用的时机和机制
    * copy -> reset -> isZero
    *
    */
  class MyWordCountAccumulator extends AccumulatorV2[String, mutable.Map[String, Int]]{

    // 存储 WordCount 的集合
    var wordCountMap = mutable.Map[String, Int]()

    // TODO : 初始值 初始化的值  判断累加器是否初始化
    override def isZero: Boolean = {
      // assertion failed: copyAndReset must return a zero value copy
      // wordCountMap.nonEmpty

      wordCountMap.isEmpty
    }

    // TODO : 复制累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
      new MyWordCountAccumulator
    }

    // TODO : 重置累加器
    override def reset(): Unit = wordCountMap.clear()

    // TODO : 向累加器中增加值
    override def add(word: String): Unit = {
      val cnt: Int = wordCountMap.getOrElse(word, 0) + 1
      wordCountMap.update(word, cnt)
    }

    // TODO : 合并累加器的值
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
      val map1 = wordCountMap
      val map2 = other.value
      wordCountMap =  map1.foldLeft(map2)(
        (map, kv) => {
          map(kv._1) = map.getOrElse(kv._1, 0) + kv._2
          map
        }
      )
    }

    // TODO : 返回累加器的值 [OUT]
    override def value: mutable.Map[String, Int] = wordCountMap

  }


}
