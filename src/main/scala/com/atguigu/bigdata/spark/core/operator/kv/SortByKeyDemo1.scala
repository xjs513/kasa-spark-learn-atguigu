package com.atguigu.bigdata.spark.core.operator.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortByKeyDemo1 {
  def main(args: Array[String]): Unit = {
    // todo : Spark 创建运行环境
    val conf: SparkConf = new SparkConf()
      .setAppName("SortByKeyDemo1")
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // TODO : sortByKey  根据 key 排序  自定义 key

    val rdd: RDD[(User, Int)] = sc.makeRDD(
      List(
        (new User(), 1),
        (new User(), 2),
        (new User(), 3)
      ), 2
    )

    val sortRDD: RDD[(User, Int)] = rdd.sortByKey(true)

    val resultRDD: RDD[Iterator[(Int, User, Int)]] = sortRDD.mapPartitionsWithIndex(
      (index: Int, iter: Iterator[(User, Int)]) => {
        iter.map {
          case (user, int) => {
            List((index, user, int)).iterator
          }
        }
      }
    )

    val array: Array[Iterator[(Int, User, Int)]] = resultRDD.collect()

    println(array)

//    resultRDD.foreach(a => {
//      a.foreach(a => println(a._1 + "  =>  " + a._2 + "  =>  " + a._3))
//    })

    sc.stop()

  }


  // 如果要自定义 key 进行排序，需要混入特质 Ordered
  class User extends Ordered[User] with Serializable {
    override def compare(that: User): Int = {
      1
    }

    override def toString: String = "User@Instance"
  }

}
