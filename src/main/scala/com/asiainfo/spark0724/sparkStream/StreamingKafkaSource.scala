package com.asiainfo.spark0724.sparkStream


import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingKafkaSource {
  //需求
  //使用 netcat 工具向 9999 端口不断的发送数据，通过 Spark Streaming 读取端口数据并统计不同单词出现的次数
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("streaming").setMaster("local[*]")
    //seconds 是一个伴生对象，durations 中有多个伴生对象，可以直接使用，里面提供了apply方法
    //TODO 拿到一个streaming 对象
    val ssc = new StreamingContext(conf,Seconds(3))
    //虽然不报错，但是没有这样的构造方法，所以编译会报错
    //通过网络得到一行行的数据
    var serializer_class="org.apache.kafka.common.serialization.StringDeserializer"
    var topic="kafkatest"
    val kafkaParams: Map[String, String] = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> "test",
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "s102:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> serializer_class,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> serializer_class
    )
//    kafkaParams
    val kafkaStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set(topic))
//kafka-topics.sh --zookeeper s102:2181,s103:2181 --create --topic kafkatest --partitions 3 --replication-factor 2
    //kafka-console-producer.sh --broker-list s102:9092 --topic kafkatest
    kafkaStream.print()

    //TODO 让采集器一直运行
    ssc.start()
    //driver 等待采集器执行完毕
    ssc.awaitTermination()


//    val value: Any = Durations(10)
  }


}
