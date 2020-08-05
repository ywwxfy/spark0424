package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Transform7 {

  def main(args: Array[String]): Unit = {

    // TODO 1. 创建Spark配置对象
    val sparkConf = new SparkConf().setAppName("Spark08_RDD_Transform5").setMaster("local[*]")

    // TODO 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // TODO 3. 构建RDD
    val numRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10))

    // 随机抽取数据
    val sampleRDD: RDD[Int] = numRDD.sample(true, 2,1)

    println(sampleRDD.collect().mkString(","))

    // TODO 9. 释放连接
    sc.stop()


  }
}
