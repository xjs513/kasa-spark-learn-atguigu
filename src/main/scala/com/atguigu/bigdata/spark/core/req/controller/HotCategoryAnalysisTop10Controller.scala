package com.atguigu.bigdata.spark.core.req.controller

import com.atguigu.bigdata.spark.core.req.service.HotCategoryAnalysisTop10Service
import com.atguigu.summer.framework.core.TController

class HotCategoryAnalysisTop10Controller extends TController{

  private val hotCategoryAnalysisTop10Service = new HotCategoryAnalysisTop10Service

  override def execute(): Unit = {
    val result = hotCategoryAnalysisTop10Service.analysis()
    result.foreach(println)
  }
}
