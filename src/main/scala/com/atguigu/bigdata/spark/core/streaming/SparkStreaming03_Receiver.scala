package com.atguigu.bigdata.spark.core.streaming

import java.io.{BufferedInputStream, BufferedReader, InputStream, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming03_Receiver {

  def main(args: Array[String]): Unit = {

    // 监听指定端口，获取数据，实现WordCount功能

    val sparkConf = new SparkConf().setAppName("SparkStreaming02_Source").setMaster("local[*]")

    // TODO 创建上下文环境对象
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    // TODO 获取离散化流
    // 使用自定义采集器采集数据
    val receiverDStream: ReceiverInputDStream[String] = ssc.receiverStream( new MySocketReceiver("linux1", 9999) )

    // TODO 将数据进行扁平化
    val wordDStream: DStream[String] = receiverDStream.flatMap( line=>line.split(" ") )

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
// 自定义采集器
// 1. 继承Receiver, 传递父类参数
// 2. 重写方法
class MySocketReceiver(host:String, port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

  var socket: Socket = null

  def receive(): Unit = {
    socket = new Socket(host, port)


    val reader = new BufferedReader(
      new InputStreamReader(
        new BufferedInputStream(
          socket.getInputStream
        ),
        "UTF-8"
      )
    )

    var s = ""

    while ( (s = reader.readLine()) != null ) {
      if ( s == "==END==" ) {

      } else {
        store(s)
      }
    }
  }

  override def onStart(): Unit = {
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }

  override def onStop(): Unit = {
    if ( socket != null ) {
      socket.close()
      socket = null;
    }
  }
}