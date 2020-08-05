package com.atguigu.bigdata.spark.core.streaming

import java.io.{BufferedInputStream, BufferedReader, InputStream, InputStreamReader}
import java.net.Socket

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming04_Kafka {

  def main(args: Array[String]): Unit = {

    // 监听指定端口，获取数据，实现WordCount功能

    val sparkConf = new SparkConf().setAppName("SparkStreaming04_Kafka").setMaster("local[*]")

    // TODO 创建上下文环境对象
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 采集Kafka数据源中的数据
    val brokers = "linux1:9092,linux2:9092,linux3:9092"
    val topic = "atguigu190513"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    val kafkaParams = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder(
      ssc, kafkaParams, Set(topic)
    )
    //kafkaDStream.map(_._2).foreachRDD(rdd=>rdd.foreach(println))
    kafkaDStream.print()

    // TODO 让采集器启动执行
    ssc.start()

    // TODO Driver等待采集器的执行完毕
    ssc.awaitTermination()
  }
}