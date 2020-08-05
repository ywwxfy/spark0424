package com.asiainfo.spark0724.sparksql

import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}

//spark 数据源
object SparkSQLSource {

  //spark session 加载数据源
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[2]").appName("sqlSoure").getOrCreate()
    //默认读取的是列式存储的文件，1.json is not a Parquet file，需要读取其他文件，需要我们传参
    //列式存储方便统计某一列，行式存储方便查询数据
//    val df: DataFrame = spark.read.load("input/1.json")
    val df: DataFrame = spark.read.format("json").load(("input/1.json"))
//    df.show()
    //保存数据,需要指定模式，是overwrite append
    df.write.mode("append").save("output")
    //TODO 释放对象
    spark.stop()
  }



}
