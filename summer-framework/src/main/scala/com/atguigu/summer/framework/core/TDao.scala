package com.atguigu.summer.framework.core

import com.atguigu.summer.framework.util.EnvUtil

trait TDao {

  def readFile(path:String) = {
    EnvUtil.getEnv().textFile(path)
  }
}
