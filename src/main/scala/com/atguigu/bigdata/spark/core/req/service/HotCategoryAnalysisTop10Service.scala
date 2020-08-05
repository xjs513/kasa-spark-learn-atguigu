package com.atguigu.bigdata.spark.core.req.service

import com.atguigu.bigdata.spark.core.acc.AccumulatorDemo03.MyWordCountAccumulator
import com.atguigu.bigdata.spark.core.req.bean.{HotCategory, HotCategoryAccumulator}
import com.atguigu.bigdata.spark.core.req.dao.HotCategoryAnalysisTop10Dao
import com.atguigu.summer.framework.core.TService
import com.atguigu.summer.framework.util.EnvUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable

class HotCategoryAnalysisTop10Service extends TService{

  private val hotCategoryAnalysisTop10Dao = new HotCategoryAnalysisTop10Dao

  /**
    * 数据分析
    *
    * @return
    */
  def analysis_bak1() = {
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

    // TODO : 对品类进行下单统计 (category, orderCount)
    val orderRDD: RDD[(String, Int)] = actionRDD.map(
      (action: String) => {
        val elements: Array[String] = action.split(",")
        elements(8)
      }
    ).filter(_ != "").flatMap(_.split("-")).map((_, 1))

    val categoryIdToOrderCountRDD: RDD[(String, Int)] = orderRDD.reduceByKey((_: Int) + (_: Int))

    // TODO : 对品类进行支付统计 (category, payCount)
    val payRDD: RDD[(String, Int)] = actionRDD.map(
      (action: String) => {
        val elements: Array[String] = action.split(",")
        elements(10)
      }
    ).filter(_ != "").flatMap(_.split("-")).map((_, 1))

    val categoryIdToPayCountRDD: RDD[(String, Int)] = payRDD.reduceByKey((_: Int) + (_: Int))

    // TODO : 将上述统计结果进行结构转换 ==****==
    // tuple => (element1,  element2, element3)
    val joinRDD: RDD[(String, (Int, Int))] = categoryIdToClickCountRDD.join(categoryIdToOrderCountRDD)
    val joinRDD1: RDD[(String, ((Int, Int), Int))] = joinRDD.join(categoryIdToPayCountRDD)
    val mapRDD: RDD[(String, (Int, Int, Int))] = joinRDD1.mapValues(a => (a._1._1, a._1._2, a._2))
    // TODO : 排序 降序
    val sortRDD: RDD[(String, (Int, Int, Int))] = mapRDD.sortBy(_._2, false)
    // TODO : 取 TOP-10
    val result: Array[(String, (Int, Int, Int))] = sortRDD.take(10)
    result
  }

  /**
    * 数据分析 性能优化版本
    *
    * @return
    */
  def analysis_bak2() = {
    // TODO : 读取日志数据
    val actionRDD: RDD[String] = hotCategoryAnalysisTop10Dao.readFile("input/user_visit_action.csv")

    // TODO : 缓存 RDD   性能优化
    actionRDD.cache()

    // TODO : 对品类进行点击统计 (category, clickCount)
    val clickRDD: RDD[(String, Int)] = actionRDD.map(
      (action: String) => {
        val elements: Array[String] = action.split(",")
        (elements(6), 1)
      }
    ).filter(_._1 != "-1")

    val categoryIdToClickCountRDD: RDD[(String, Int)] = clickRDD.reduceByKey((_: Int) + (_: Int))

    // TODO : 对品类进行下单统计 (category, orderCount)
    val orderRDD: RDD[(String, Int)] = actionRDD.map(
      (action: String) => {
        val elements: Array[String] = action.split(",")
        elements(8)
      }
    ).filter(_ != "").flatMap(_.split("-")).map((_, 1))

    val categoryIdToOrderCountRDD: RDD[(String, Int)] = orderRDD.reduceByKey((_: Int) + (_: Int))

    // TODO : 对品类进行支付统计 (category, payCount)
    val payRDD: RDD[(String, Int)] = actionRDD.map(
      (action: String) => {
        val elements: Array[String] = action.split(",")
        elements(10)
      }
    ).filter(_ != "").flatMap(_.split("-")).map((_, 1))

    val categoryIdToPayCountRDD: RDD[(String, Int)] = payRDD.reduceByKey((_: Int) + (_: Int))

    // TODO : 将上述统计结果进行结构转换 ==****==
    // tuple => (element1,  element2, element3)
    /**
      * (品类, 点击数量), (品类, 下单数量), (品类, 支付数量)
      * (品类, (点击数量,0, 0)), (品类, (0, 下单数量, 0)), (品类, (0, 0, 支付数量))
      * (品类, (点击数量，下单数量，支付数量))
      */

    val newCategoryIdToClickCountRDD: RDD[(String, (Int, Int, Int))] = categoryIdToClickCountRDD.map {
      case (id, clickCount) => (id, (clickCount, 0, 0))
    }

    val newCategoryIdToOrderCountRDD: RDD[(String, (Int, Int, Int))] = categoryIdToOrderCountRDD.map {
      case (id, orderCount) => (id, (0, orderCount, 0))
    }

    val newCategoryIdToPayCountRDD: RDD[(String, (Int, Int, Int))] = categoryIdToPayCountRDD.map {
      case (id, payCount) => (id, (0, 0, payCount))
    }

    val unionRDD: RDD[(String, (Int, Int, Int))] = newCategoryIdToClickCountRDD.union(newCategoryIdToOrderCountRDD).union(newCategoryIdToPayCountRDD)

    val reductRDD: RDD[(String, (Int, Int, Int))] = unionRDD.reduceByKey {
      (tuple1, tuple2) => {
        (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2, tuple1._3 + tuple2._3)
      }
    }


    // TODO : 排序 降序
    val sortRDD: RDD[(String, (Int, Int, Int))] = reductRDD.sortBy(_._2, false)
    // TODO : 取 TOP-10
    val result: Array[(String, (Int, Int, Int))] = sortRDD.take(10)

    /**
      * (20,(3104,0,0))
      * (15,(3098,0,0))
      * (17,(3080,0,0))
      * (14,(3079,0,0))
      * (10,(3060,0,0))
      * (13,(3056,0,0))
      * (2,(3054,6044,4046))
      * (5,(3032,0,0))
      * (3,(3024,6044,4046))
      * (7,(3020,0,0))
      */
    result
  }
  /**
    * 数据分析 性能优化版本
    *
    * @return
    */
  def analysis_bak3() = {
    // TODO : 读取日志数据
    val actionRDD: RDD[String] = hotCategoryAnalysisTop10Dao.readFile("input/user_visit_action.csv")

    // TODO : 对品类进行点击统计 (category, clickCount)
    val flatMapRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
      (action: String) => {
        val elements: Array[String] = action.split(",")
        if (elements(6) != "-1") {
          // 说明这是点击事件
          List((elements(6), (1, 0, 0)))
        } else if (elements(8) != "") {
          // 说明这是下单事件
          elements(8).split("-").map((_, (0, 1, 0)))
        } else if (elements(10) != "") {
          // 说明这是支付事件
          elements(10).split("-").map((_, (0, 0, 1)))
        } else {
          //List(("", (0, 0, 0)))
          Nil
        }
      }
    )
    val reduceRDD: RDD[(String, (Int, Int, Int))] = flatMapRDD.reduceByKey {
      (tuple1, tuple2) => {
        (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2, tuple1._3 + tuple2._3)
      }
    }
    // TODO : 排序 降序
    val sortRDD: RDD[(String, (Int, Int, Int))] = reduceRDD.sortBy(_._2, false)
    // TODO : 取 TOP-10
    val result: Array[(String, (Int, Int, Int))] = sortRDD.take(10)

    /**
      * (20,(3104,0,0))
      * (15,(3098,0,0))
      * (17,(3080,0,0))
      * (14,(3079,0,0))
      * (10,(3060,0,0))
      * (13,(3056,0,0))
      * (2,(3054,6044,4046))
      * (5,(3032,0,0))
      * (3,(3024,6044,4046))
      * (7,(3020,0,0))
      */
    result
  }
  /**
    * 数据分析 性能优化版本
    *
    * @return
    */
  def analysis_bak4() = {
    // TODO : 读取日志数据
    val actionRDD: RDD[String] = hotCategoryAnalysisTop10Dao.readFile("input/user_visit_action.csv")

    // TODO : 对品类进行点击统计 (category, clickCount)
    val flatMapRDD: RDD[(String, HotCategory)] = actionRDD.flatMap(
      (action: String) => {
        val elements: Array[String] = action.split(",")
        if (elements(6) != "-1") {
          // 说明这是点击事件
          List((elements(6),HotCategory(elements(6), 1, 0, 0)))
        } else if (elements(8) != "") {
          // 说明这是下单事件
          elements(8).split("-").map(id => (id, HotCategory(id, 0, 1, 0)))
        } else if (elements(10) != "") {
          // 说明这是支付事件
          elements(10).split("-").map(id => (id, HotCategory(id, 0, 0, 1)))
        } else {
          Nil
        }
      }
    )
    val reduceRDD: RDD[(String, HotCategory)] = flatMapRDD.reduceByKey {
      (a, b) => {
        HotCategory(a.categoryId, a.clickCount + b.clickCount, a.orderCount + b.orderCount, a.payCount + b.payCount)
      }
    }

    reduceRDD.collect().sortWith{
      (left, right) => {
        val leftHC = left._2
        val rightHC = right._2
        if (leftHC.clickCount > rightHC.clickCount) {
          true
        } else if (leftHC.clickCount == rightHC.clickCount){
          if (leftHC.orderCount > rightHC.orderCount) {
            true
          } else if (leftHC.orderCount == rightHC.orderCount){
            leftHC.payCount > rightHC.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    }.take(10)

    /**
      * (20,HotCategory(20,3104,0,0))
      * (15,HotCategory(15,3098,0,0))
      * (17,HotCategory(17,3080,0,0))
      * (14,HotCategory(14,3079,0,0))
      * (10,HotCategory(10,3060,0,0))
      * (13,HotCategory(13,3056,0,0))
      * (2,HotCategory(2,3054,6044,4046))
      * (5,HotCategory(5,3032,0,0))
      * (3,HotCategory(3,3024,6044,4046))
      * (7,HotCategory(7,3020,0,0))
      */
  }
  /**
    * 数据分析 性能优化版本
    *
    * @return
    */
  override def analysis() = {
    // TODO : 读取日志数据
    val actionRDD: RDD[String] = hotCategoryAnalysisTop10Dao.readFile("input/user_visit_action.txt")

    // TODO : 对品类进行点击统计 (category, clickCount)
    val flatMapRDD: RDD[(String, HotCategory)] = actionRDD.flatMap(
      (action: String) => {
        val elements: Array[String] = action.split("_")
        if (elements(6) != "-1") {
          // 说明这是点击事件
          List((elements(6),HotCategory(elements(6), 1, 0, 0)))
        } else if (elements(8) != "null") {
          // 说明这是下单事件
          elements(8).split(",").map(id => (id, HotCategory(id, 0, 1, 0)))
        } else if (elements(10) != "null") {
          // 说明这是支付事件
          elements(10).split(",").map(id => (id, HotCategory(id, 0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    // TODO : 使用累加器对数据进行聚合

    // 1. 创建自定义累加器
    val accumulator = new HotCategoryAccumulator

    // 2. 注册自定义累加器
    EnvUtil.getEnv().register(accumulator)

    // 3. 使用累加器
    flatMapRDD.foreach {
      word => {
        accumulator.add(word._2)
      }
    }

    // 4. 获取累加器的值
    val value: mutable.Map[String, HotCategory] = accumulator.value

    val iterable: mutable.Iterable[HotCategory] = value.map(_._2)

    value.map(_._2).toList.sortWith{
      (leftHC, rightHC) => {
        if (leftHC.clickCount > rightHC.clickCount) {
          true
        } else if (leftHC.clickCount == rightHC.clickCount){
          if (leftHC.orderCount > rightHC.orderCount) {
            true
          } else if (leftHC.orderCount == rightHC.orderCount){
            leftHC.payCount > rightHC.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    }.take(10)
  }
}
