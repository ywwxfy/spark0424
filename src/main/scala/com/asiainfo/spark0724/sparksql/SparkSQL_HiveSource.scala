package com.asiainfo.spark0724.sparksql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

//spark 数据源
object SparkSQL_HiveSource {

  //spark session 加载数据源
  def main(args: Array[String]): Unit = {
    //启用对hive 的支持
    val spark: SparkSession = SparkSession.builder()
//      .config("spark.sql.warehouse.dir", "hdfs://s102:8020/user/hive/warehouse")
      .master("local[2]")
      .appName("hive")
      .enableHiveSupport()
      .getOrCreate()
  import spark.implicits._
    import spark.sql
    sql("select * from gmall.ads_uv_count").show

//    val df: DataFrame = spark.read.format("jdbc")
////      .option(
////        "url","jdbc:mysql://192.168.137.106:3306/mydb"
////      ).option("user","root")
////      .option("password","Dhy00@222")
////      .option("dbtable","mytbl")
////      .load()
    //jdbc 连接方式连接数据库
//    val prop = new Properties()
//    prop.setProperty("user","root")
//    prop.setProperty("password","Dhy00@222")
//    val df: DataFrame = spark.read.jdbc("jdbc:mysql://192.168.137.106:3306/mydb","mytbl",prop)
//      df.show()

    //TODO 释放对象
    spark.stop()
  }



}
