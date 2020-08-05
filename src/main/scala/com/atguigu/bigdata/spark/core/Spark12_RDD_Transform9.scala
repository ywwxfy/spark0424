package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Transform9 {

  def main(args: Array[String]): Unit = {

    // TODO 1. 创建Spark配置对象
    val sparkConf = new SparkConf().setAppName("Spark08_RDD_Transform5").setMaster("local[*]")

    // TODO 2. 创建Spark环境连接对象
    val sc = new SparkContext(sparkConf)

    // TODO 3. 构建RDD
    val numRDD = sc.parallelize(1 to 10, 5)
   // val array: Array[Array[Int]] = numRDD.glom().collect()
    //array.foreach(a=> println(a.mkString(",")))
    //println(numRDD.partitions.length)

    // 合并分区: 1) 不shuffle， 2) 有shuffle
    //val coalesceRDD: RDD[Int] = numRDD.coalesce(2,true)
    // 再分区，存在shuffle，从源码的角度，其实就是有shuffle的coalesce操作.
    val repartitionRDD: RDD[Int] = numRDD.repartition(2)
    val array1: Array[Array[Int]] = repartitionRDD.glom().collect()
    array1.foreach(a=> println(a.mkString(",")))

    // TODO 9. 释放连接
    sc.stop()


  }
}
