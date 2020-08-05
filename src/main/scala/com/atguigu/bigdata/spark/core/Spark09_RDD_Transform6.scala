package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_Transform6 {

  def main(args: Array[String]): Unit = {

    // TODO 1. 创建Spark配置对象
    val sparkConf = new SparkConf().setAppName("Spark08_RDD_Transform5").setMaster("local[*]")

    // TODO 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // TODO 3. 构建RDD
    val numRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    // 转换算子 - groupBy

    // TODO 4. 根据奇偶数进行分组
    //val groupByRDD: RDD[(Int, Iterable[Int])] = numRDD.groupBy(num=>num%2)

    //println(groupByRDD.collect().mkString(","))

    // 转换算子 - filter
    val filterRDD: RDD[Int] = numRDD.filter(num=>num%2 != 0)
    println(filterRDD.collect().mkString(","))

    // TODO 9. 释放连接
    sc.stop()


  }
}
