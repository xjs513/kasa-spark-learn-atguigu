package com.atguigu.bigdata.spark.core.req.controller

import com.atguigu.bigdata.spark.core.req.service.WordCountService
import com.atguigu.summer.framework.core.TController

/**
  * wordCount 控制器
  */
class WordCountController extends TController {


  private val wordCountService = new WordCountService

  override def execute(): Unit = {
    val result = wordCountService.analysis()
    result.foreach(println)
  }
}
