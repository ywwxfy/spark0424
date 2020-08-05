package com.asiainfo.spark0724.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkSql")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val df: DataFrame = spark.read.json("input/1.json")
//    val df: DataFrame = spark.read.json("input/agent.log")
//    df.show()
    //导入spark 对象的隐式转换所有方法
    import spark.implicits._
df.filter($"age">30).show
    df.createTempView("user")
    spark.sql("select * from user").show()

    //释放连接
    spark.stop()
  }


}
