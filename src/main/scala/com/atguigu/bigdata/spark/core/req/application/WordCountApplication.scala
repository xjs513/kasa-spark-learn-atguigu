package com.atguigu.bigdata.spark.core.req.application

import com.atguigu.bigdata.spark.core.req.controller.WordCountController
import com.atguigu.summer.framework.core.TApplication

object WordCountApplication extends TApplication {
  def main(args: Array[String]): Unit = {
    start("spark"){
      val controller = new WordCountController
      controller.execute()
    }
  }
}
