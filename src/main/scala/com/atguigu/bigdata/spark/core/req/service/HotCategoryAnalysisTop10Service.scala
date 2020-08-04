package com.atguigu.bigdata.spark.core.req.service

import com.atguigu.bigdata.spark.core.req.dao.HotCategoryAnalysisTop10Dao
import com.atguigu.summer.framework.core.TService
import org.apache.spark.rdd.RDD

class HotCategoryAnalysisTop10Service extends TService{

  private val hotCategoryAnalysisTop10Dao = new HotCategoryAnalysisTop10Dao

  /**
    * 数据分析
    *
    * @return
    */
  override def analysis() = {
    // TODO : 读取日志数据
    val actionRDD: RDD[String] = hotCategoryAnalysisTop10Dao.readFile("input/user_visit_action.csv")
    // TODO : 对品类进行点击统计 (category, clickCount)
    val clickRDD: RDD[(String, Int)] = actionRDD.map(
      (action: String) => {
        val elements: Array[String] = action.split(",")
        (elements(6), 1)
      }
    ).filter(_._1 != "-1")

    val categoryIdToClickCountRDD: RDD[(String, Int)] = clickRDD.reduceByKey((_: Int) + (_: Int))

    /**
    (20,3104)
    (15,3098)
    (17,3080)
    (14,3079)
    (10,3060)
    (13,3056)
    (2,3054)
    (5,3032)
    (3,3024)
    (7,3020)
    (19,3012)
    (1,3005)
    (8,2995)
    (4,2993)
    (18,2990)
    (6,2958)
    (9,2953)
    (12,2932)
    (11,2920)
    (16,2897)
      */

//    val value: RDD[(String, Int)] = categoryIdToClickCountRDD.sortBy(_._2, false)
//
//    value.collect().foreach(println)

//    // TODO : 对品类进行下单统计 (category, orderCount)
//    val orderRDD: RDD[(String, Int)] = actionRDD.map(
//      (action: String) => {
//        val elements: Array[String] = action.split(",")
//        elements(8)
//      }
//    ).filter(_ != "").flatMap(_.split("-")).map((_, 1))
//
//    val categoryIdToOrderCountRDD: RDD[(String, Int)] = orderRDD.reduceByKey((_: Int) + (_: Int))
//
//    // TODO : 对品类进行支付统计 (category, payCount)
//    val payRDD: RDD[(String, Int)] = actionRDD.map(
//      (action: String) => {
//        val elements: Array[String] = action.split(",")
//        elements(10)
//      }
//    ).filter(_ != "").flatMap(_.split("-")).map((_, 1))
//
//    val categoryIdToPayCountRDD: RDD[(String, Int)] = payRDD.reduceByKey((_: Int) + (_: Int))
//
//    // TODO : 将上述统计结果进行结构转换 ==****==
//    // tuple => (element1,  element2, element3)
//    val joinRDD: RDD[(String, (Int, Int))] = categoryIdToClickCountRDD.join(categoryIdToOrderCountRDD)
//    val joinRDD1: RDD[(String, ((Int, Int), Int))] = joinRDD.join(categoryIdToPayCountRDD)
//    val mapRDD: RDD[(String, (Int, Int, Int))] = joinRDD1.mapValues(a => (a._1._1, a._1._2, a._2))
//    // TODO : 排序 降序
//    val sortRDD: RDD[(String, (Int, Int, Int))] = mapRDD.sortBy(_._2, false)
//    // TODO : 取 TOP-10
//    val result: Array[(String, (Int, Int, Int))] = sortRDD.take(10)
//    result
  }
}
