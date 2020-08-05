package com.asiainfo.spark0724.sparkStream

import java.io._
import java.net.{ConnectException, Socket}
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.NonFatal

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
    val receiver = MySocketReceiver("s102",9999)
    val lines: ReceiverInputDStream[String] = ssc.receiverStream(receiver)
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
//自定义一个 socketStream
class MySocketReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  //不知道初始值，给类型和一个下划线就解决,下划线表示给属性一个默认值
//  private var socket:Socket = null
  private var socket:Socket = _
  override def onStart(): Unit = {
    try {

      socket = new Socket(host, port)
    }catch {
        case e: ConnectException =>
          restart(s"Error connecting to $host:$port", e)
          return
      }

      // Start the thread that receives data over a connection，匿名内部类
      new Thread("MySocket Receiver") {
        setDaemon(true)
        override def run() {
          receive()
        }
      }.start()

    }


  override def onStop(): Unit = {
    if (socket!=null){
      socket.close()
      socket=null
    }
  }

  def receive(): Unit ={
    var bufferedReader:BufferedReader = null

    try {
        val stream: InputStream = socket.getInputStream()
        //封装得到一个reader
//        val bufferedReader = new BufferedReader(new InputStreamReader(stream,StandardCharsets.UTF_8))
        //使用一个缓冲流，提高读取速度
        bufferedReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(stream),"utf-8"))
        //读取数据
        var s=""
        while ((s=bufferedReader.readLine())!=null) {
          if(s=="==end=="){

          }else{
            store(s)
          }
        }
//        while (!isStopped()&& bufferedReader.readLine()!=null) {

        //        }
        if (!isStopped()) {
          restart("Socket data stream had no more data")
        } else {

        }
    } catch {
      case NonFatal(e) =>
        restart("Error receiving data", e)
    } finally {
      if (bufferedReader!=null){

        bufferedReader.close()
      }
      onStop()
    }
  }


}
//伴生对象提供apply 方法
object MySocketReceiver{

  def apply(host:String,port:Int): MySocketReceiver= new MySocketReceiver(host,port)
}

