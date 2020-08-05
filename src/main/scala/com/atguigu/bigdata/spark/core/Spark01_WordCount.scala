package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {

  def main(args: Array[String]): Unit = {

    // 创建第一个Spark应用程序 : WordCount

    // TODO 1. 创建Spark配置对象
    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // TODO 2. 创建Spark环境连接对象
    // 创建Spark上下文环境对象的类，称之为Driver类
    // Spark-shell就是Driver
    val sc = new SparkContext(sparkConf)

    // TODO 3. 读取文件:
    //  3.1)从classpath中获取
//     Thread.currentThread().getContextClassLoader().getResourceAsStream()
    //  3.2) 从项目的环境中获取,那文件路径就在项目的根目录下
    val lineRDD: RDD[String] = sc.textFile("data/input")

    // TODO 4. 将每一行的字符串拆分成一个一个的单词(扁平化)
    val wordRDD: RDD[String] = lineRDD.flatMap(line=>{line.split(' ')})

    // TODO 5. 将单词转换结构，用于后续的统计
    val wordToOneRDD: RDD[(String, Int)] = wordRDD.map(word=>(word,1))

    // TODO 6. 将数据进行分组聚合
    // RDD类中不存在reduceByKey方法，但是如果想要使用这个方法，需要隐式转换
    val wordToCountRDD: RDD[(String, Int)] = wordToOneRDD.reduceByKey((x,y)=>{x+y})

    // TODO 7. 将数据统计结果收集到Driver端进行展示
    val result: Array[(String, Int)] = wordToCountRDD.collect()

    // TODO 8. 循环遍历展示结果
    result.foreach(println)

    // TODO 9. 释放连接
    sc.stop()


  }
}
