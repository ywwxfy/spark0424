package com.asiainfo.spark0724.spark.core

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HbaseWriteData {
  //连接mysql数据库
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("hbase").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
        val hbaseConf: Configuration = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","s102,s103,s104")
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE,"member")
    //向hbase中插入数据
    val dataRDD: RDD[(String, String, String)] = sc.makeRDD(Array(("cuteywakdkas","36","chengdu"),("cutessyyewkdkas","28","sichuan")))
    val job: Job = Job.getInstance(hbaseConf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])

    val insertRDD: RDD[(ImmutableBytesWritable, Put)] = dataRDD.map {
      case (rowkey, age, city) => {
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("address"), Bytes.toBytes("age"), Bytes.toBytes(age))
        put.addColumn(Bytes.toBytes("address"), Bytes.toBytes("city"), Bytes.toBytes(city))
        (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
      }
    }
    insertRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)




    //释放sc
    sc.stop
  }
}
