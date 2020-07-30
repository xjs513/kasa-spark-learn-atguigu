package com.atguigu.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object GroupByDemo {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("GroupByDemo")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 3)
    // TODO : groupBy
    // 根据指定的规则将数据分组,指定规则的返回值就时分组的key,value 为元素的可迭代集合
    // 分区默认不变，但数据会打乱重新组合，这叫做洗牌 shuffle
    // shuffle 可能导致数据不均匀的情况，可以改变下游 RDD 的分区数 ** 注意调用
    // 极限情况下，数据可能进入一个分区中
    // 一个分组的数据都在一个分区中，并不是说一个分区就就只能有一个分组

    // dataRDD.groupBy(num => num%2, 2) // 不能这样调用

    val rdd: RDD[(Int, Iterable[Int])] = dataRDD.groupBy((num:Int) => num%2, 2)




    // rdd.saveAsTextFile("data\\output\\groupBy")

    println("分组后的分区数量: " + rdd.glom().collect().length)


    rdd.collect.foreach{
      case (key, list) => {
        println("key:" + key + " list = [" + list.mkString(", ") + "]")
      }
    }

    // TODO : 计算所有分区的最大值求和，分区内取最大值，分区间最大值求和

    // TODO : filter 算子 过滤 不再赘述~！

//    val maxPerPartition: RDD[Int] = rdd.map(_.max)
//    val sum: Int = maxPerPartition.reduce(_ + _)
//    println("sum = " + sum)

    sc.stop()
  }
}
