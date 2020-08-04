package com.atguigu.bigdata.spark.core.req.service

import com.atguigu.bigdata.spark.core.req.dao.WordCountDao
import com.atguigu.summer.framework.core.TService

class WordCountService extends TService {

   private val wordCountDao = new WordCountDao

  /**
    * 数据分析
    * envData 采用的这样的方式不是最优的方法
    * 线程中一致就保留着可以共享数据的内存空间
    * JDK API 中提供了工具类可以访问这块内存空间： ThreadLocal
    * @return
    */
  override def analysis() = {
    val fileRDD = wordCountDao.readFile("data\\input\\spark_01\\a.txt")

    val resultRDD = fileRDD.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    resultRDD.collect()
  }
}
