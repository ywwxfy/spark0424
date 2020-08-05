package com.atguigu.bigdata.spark.core.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object SparkStreaming01_WordCount {

  def main(args: Array[String]): Unit = {

    // 监听指定端口，获取数据，实现WordCount功能

    val sparkConf = new SparkConf().setAppName("SparkStreaming01_WordCount").setMaster("local[2]")

    // TODO 创建上下文环境对象
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    // TODO 获取离散化流
    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("linux1", 9999)

    // TODO 将数据进行扁平化
    val wordDStream: DStream[String] = socketDStream.flatMap( line=>line.split(" ") )

    // TODO 将数据转换结构（ word ） => (word, 1)
    val wordToOneDStream: DStream[(String, Int)] = wordDStream.map( word=>(word,1) )

    // TODO 将转换结构后的数据进行聚合
    val wordToSumDStream: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_+_)

    // TODO 将数据开始执行操作
    wordToSumDStream.print()

    // TODO 让采集器启动执行
    ssc.start()

    // TODO Driver等待采集器的执行完毕
    ssc.awaitTermination()
  }
}
