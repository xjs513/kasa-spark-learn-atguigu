package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL09_Test {
  def main(args: Array[String]): Unit = {

    // 解决 HDFS 权限问题
    System.setProperty("HADOOP_USER_NAME", "root")

    // TODO : 创建运行上下文
    val conf: SparkConf = new SparkConf().setMaster("local[*]")
      .setAppName("SparkSQL09_Test")
    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()
    // TODO : 导入隐式转换 这里的 spark 是刚创建的环境对象 必须用 val 声明
    // import spark.implicits._

//    val df: DataFrame = spark.sql(
//      """
//        |select
//        |  area,
//        |  product_name,
//        |  count(1) as cnt
//        |from (
//        |  select
//        |    a.*,
//        |    c.area,
//        |    p.product_name
//        |  from user_visit_action a
//        |  join city_info c on c.city_id = a.city_id
//        |  join product_info p on p.product_id = a.click_product_id
//        |  where a.click_product_id > -1
//        |) group by area, product_name
//      """.stripMargin)

    val df: DataFrame = spark.sql(
      """
        |select
        |  *
        |from (
        |  select
        |    t.area,
        |    t.product_name,
        |    t.cnt,
        |    Row_Number() OVER (partition by t.area ORDER BY t.cnt desc) rank
        |  from (
        |    select
        |      area,
        |      product_name,
        |      count(1) as cnt
        |    from (
        |      select
        |        a.*,
        |        c.area,
        |        p.product_name
        |      from user_visit_action a
        |      join city_info c on c.city_id = a.city_id
        |      join product_info p on p.product_id = a.click_product_id
        |      where a.click_product_id > -1
        |    ) group by area, product_name
        |  ) t
        |) where rank < 4
      """.stripMargin)


    df.show(1000,false)

    spark.stop()
  }
}


















