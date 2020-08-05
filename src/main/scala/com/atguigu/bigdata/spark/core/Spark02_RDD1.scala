package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD1 {

  def main(args: Array[String]): Unit = {

    // 创建第一个Spark应用程序 : WordCount

    // TODO 1. 创建Spark配置对象
    val sparkConf = new SparkConf().setAppName("Spark02_RDD1").setMaster("local[*]")

    // TODO 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // 创建RDD : 从内存中创建
    // 并行
    // 1. parallelize
    //val rdd: RDD[Int] = sc.parallelize(List(1,2,3,4))

    // 2. makeRDD, 底层代码调用parallelize
    // 推荐使用
    //val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    // 在创建RDD的同时，指定数据分区的数量，其实就是设定第二个参数
    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4,5), 3)

    // TODO 3. 将数据保存到指定路径中
    // 保存文件时，会按照默认的分区数量进行保存
    rdd1.saveAsTextFile("output")



    // TODO 9. 释放连接
    sc.stop()


  }
}
