package com.atguigu.bigdata.spark.core.req.application

import com.atguigu.bigdata.spark.core.req.controller.HotCategoryAnalysisTop10Controller
import com.atguigu.summer.framework.core.TApplication

object HotCategoryAnalysisTop10Application extends TApplication{
  // TODO : 热门品类前10应用程序
  def main(args: Array[String]): Unit = {
    start("spark"){
      val controller = new HotCategoryAnalysisTop10Controller
      controller.execute()
    }
  }
}
