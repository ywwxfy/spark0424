package com.asiainfo.spark0724.sparksql

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

//spark 数据源
object SparkSQL_JDBCSource {

  //spark session 加载数据源
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[2]").appName("jdbc").getOrCreate()
    import spark.implicits._

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

    //向mysql 数据库写入数据
    val prop=new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","000000")
//    val df: DataFrame = spark.read.jdbc("jdbc:mysql://s102:3306/test","staff2",prop)
//    df.show()
//        val prop=new Properties()
//    prop.setProperty("user","root")
//    prop.setProperty("password","000000")
//    val df: DataFrame = spark.read.jdbc("jdbc:mysql://s102:3306/test","staff2",prop)
//    df.show()
    var jdbcUrl="jdbc:mysql://s102:3306/test"
//    df.write.mode(SaveMode.Append).jdbc(jdbcUrl,"test1",prop)
    val staffData: RDD[(Int, String, String)] = spark.sparkContext.makeRDD(Array((12,"liuyi","male"),(13,"liuyi2","male")))
    //方式一 rdd 转化为df，需要增加相应的类型就可以得到
//    val df: DataFrame = staffData.toDF("id","name","sex")
//    df.write.mode(SaveMode.Overwrite).jdbc(jdbcUrl,"test1",prop)
    //方式二 通过反射转化得到 df
    val staffCase: RDD[Staff] = staffData.map(t=>Staff(t._1,t._2,t._3))
    val df: DataFrame = staffCase.toDF()
    df.write.mode(SaveMode.Overwrite).jdbc(jdbcUrl,"test1",prop)

    //RDD DF DS之间的转化
    val df1: DataFrame = staffData.toDF("id","name","sex")
    //df转为rdd
    val rdd: RDD[Row] = df1.rdd
    //rdd 转化为ds ,需要加类型和对象，用案例类封装
    val staffRDD: RDD[Staff] = staffData.map {
      case (id, name, sex) => Staff(id, name, sex)
    }
    val ds: Dataset[Staff] = staffRDD.toDS()
    //ds转化为rdd
    val rdd3: RDD[Staff] = ds.rdd

    //df转为ds,增加一个实例类
    val ds3: Dataset[Staff] = df1.as[Staff]
    val df3: DataFrame = ds3.toDF()



    //TODO 释放对象
    spark.stop()
  }

case class Staff(id:Int,name:String,sex:String)

}
