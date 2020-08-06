package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL08_Test {
  def main(args: Array[String]): Unit = {
    // TODO : 创建运行上下文
    val conf: SparkConf = new SparkConf().setMaster("local[*]")
      .setAppName("SparkSQL08_Test")
    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()
    // TODO : 导入隐式转换 这里的 spark 是刚创建的环境对象 必须用 val 声明
    // import spark.implicits._

    // spark.sql("create table aa(id int)")

//    spark.sql(
//      """
//        |CREATE TABLE `user_visit_action`(
//        |  `date` string,
//        |  `user_id` bigint,
//        |  `session_id` string,
//        |  `page_id` bigint,
//        |  `action_time` string,
//        |  `search_keyword` string,
//        |  `click_category_id` bigint,
//        |  `click_product_id` bigint,
//        |  `order_category_ids` string,
//        |  `order_product_ids` string,
//        |  `pay_category_ids` string,
//        |  `pay_product_ids` string,
//        |  `city_id` bigint)
//        |row format delimited fields terminated by '\t'
//      """.stripMargin)

    spark.sql(
      """
        |load data local inpath 'SparkSQL_input/user_visit_action.txt' overwrite INTO table user_visit_action
      """.stripMargin)

//    spark.sql(
//      """
//        |CREATE TABLE `product_info`(
//        |  `product_id` bigint,
//        |  `product_name` string,
//        |  `extend_info` string)
//        |row format delimited fields terminated by '\t'
//      """.stripMargin)

//    spark.sql(
//      """
//        |load data local inpath 'input/product_info.txt' into table product_info
//      """.stripMargin)

//    spark.sql(
//      """
//        |CREATE TABLE `city_info`(
//        |  `city_id` bigint,
//        |  `city_name` string,
//        |  `area` string)
//        |row format delimited fields terminated by '\t'
//      """.stripMargin)

//    spark.sql(
//      """
//        |load data local inpath 'input/city_info.txt' into table city_info
//      """.stripMargin)

    spark.stop()
  }
}
