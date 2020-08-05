package com.asiainfo.spark0724.spark.core

import java.sql.{Driver, DriverManager}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object MysqlJdbcConnection {
  //连接mysql数据库
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("mysql").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    //从mysql中读取数据
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://s102:3306/test"
    val userName = "root"
    val passWd = "000000"
    var sql="select * from staff where id>=? and id<=?"
    val jdbc: JdbcRDD[String] = new JdbcRDD(sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, userName, passWd)

      },
      sql,
      1,
      3,
      3,
      (rs)=>rs.getInt(1)+","+rs.getString(2)+","+rs.getString(3)
    )
//    jdbc,每个分区取一条数据，所以需要占位符
    jdbc.collect().foreach(println)

    //释放sc
    sc.stop
  }
}
