package com.asiainfo.spark0724.spark.core

import java.sql.DriverManager

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.ByteWritable
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object HbaseReadData {
  //连接mysql数据库
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("hbase").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
        val hbaseConf: Configuration = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","s102,s103,s104")
    hbaseConf.set(TableInputFormat.INPUT_TABLE,"member")
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    val rowRDD: RDD[String] = hbaseRDD.map(t=>Bytes.toString(t._2.getRow))
    rowRDD.foreach(println)

    //释放sc
    sc.stop
  }
}
