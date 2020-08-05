package com.asiainfo.spark0724.sparkStream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Durations, Seconds, StreamingContext}

object StreamingWordcount {
  //需求
  //使用 netcat 工具向 9999 端口不断的发送数据，通过 Spark Streaming 读取端口数据并统计不同单词出现的次数
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("streaming").setMaster("local[*]")
    //seconds 是一个伴生对象，durations 中有多个伴生对象，可以直接使用，里面提供了apply方法
    //TODO 拿到一个streaming 对象
    val ssc = new StreamingContext(conf,Seconds(3))
    //虽然不报错，但是没有这样的构造方法，所以编译会报错
    //通过网络得到一行行的数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("s102",9999)
    val words: DStream[String] = lines.flatMap(_.split(' '))
    val wordsToOne: DStream[(String, Int)] = words.map((_,1))
    val wordToCountDStream: DStream[(String, Int)] = wordsToOne.reduceByKey(_+_)
    wordToCountDStream.print()

    //TODO 让采集器一直运行
    ssc.start()
    //driver 等待采集器执行完毕
    ssc.awaitTermination()


//    val value: Any = Durations(10)
  }


}
