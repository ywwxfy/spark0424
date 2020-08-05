package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Transform1 {

  def main(args: Array[String]): Unit = {

    // TODO 1. 创建Spark配置对象
    val sparkConf = new SparkConf().setAppName("Spark02_RDD2").setMaster("local[*]")

    // TODO 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // TODO 3. 构建RDD
    val numRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    // 将RDD通过算子进行转换，变成新的RDD
    // 转换算子 - map
    val mapRDD: RDD[Int] = numRDD.map(num=>{num*2})
    //mapRDD.collect().foreach(println)
    mapRDD.saveAsTextFile("output")

    // TODO 9. 释放连接
    sc.stop()


  }
}
