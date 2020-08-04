package com.atguigu.bigdata.spark.core.req

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

package object bean {
  case class HotCategory (categoryId:String,clickCount:Int,orderCount:Int,payCount:Int)

  class HotCategoryAccumulator extends AccumulatorV2[HotCategory, mutable.Map[String, HotCategory]]{

    // 存储 WordCount 的集合
    var hcMap = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = hcMap.isEmpty

    override def copy(): AccumulatorV2[HotCategory, mutable.Map[String, HotCategory]] = new HotCategoryAccumulator

    override def reset(): Unit = hcMap.clear()

    override def add(hc: HotCategory): Unit = {
      val category: HotCategory = hcMap.getOrElse(hc.categoryId, HotCategory(hc.categoryId, 0, 0, 0))
      hcMap.update(hc.categoryId, HotCategory(hc.categoryId, hc.clickCount + category.clickCount, hc.orderCount + category.orderCount, hc.payCount + category.payCount))
    }

    override def merge(other: AccumulatorV2[HotCategory, mutable.Map[String, HotCategory]]): Unit = {
      val map1 = hcMap
      val map2 = other.value
      hcMap =  map1.foldLeft(map2)(
        (map, kv) => {
          val category: HotCategory = map.getOrElse(kv._1, HotCategory(kv._1, 0, 0, 0))
          map(kv._1) = HotCategory(kv._1, kv._2.clickCount + category.clickCount, kv._2.orderCount + category.orderCount, kv._2.payCount + category.payCount)
          map
        }
      )
    }

    override def value: mutable.Map[String, HotCategory] = hcMap
  }
}
