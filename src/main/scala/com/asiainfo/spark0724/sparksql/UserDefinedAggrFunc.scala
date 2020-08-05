package com.asiainfo.spark0724.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object UserDefinedAggrFunc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkSql")
    //Todo 创建 sparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val df: DataFrame = spark.read.json("input/1.json")
    df.createTempView("people")

    spark.udf.register("avgAge",new UDFAvrag)
    spark.sql("select avgAge(age) from people").show


    //释放连接
    spark.stop()
  }


}
//自定义一个求平均值的函数
class UDFAvrag extends UserDefinedAggregateFunction{
  //输入类型,数组，seq 或者list
  override def inputSchema: StructType = StructType(Array(StructField("age",LongType)))
//缓冲区里面的类型，一个sum，一个count
  override def bufferSchema: StructType = {
    StructType(Array(StructField("sum",LongType),StructField("count",LongType)))
  }
  //结果的数据类型
  override def dataType: DataType = {
    DoubleType
  }
  // 稳定性：传递一个值到聚合函数中，返回结果是否相同（幂等性）
  override def deterministic: Boolean = true
//初始化,类型要匹配
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0L
    buffer(1)=0L
  }
 //更新逻辑，缓冲区的数据增加，传进来的参数只有一个，第二个参数是统计出现的次数
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0)=buffer.getLong(0)+input.getLong(0)
    buffer(1)=buffer.getLong(1)+1L
  }
//合并缓冲区的数据
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getLong(0)+buffer2.getLong(0)
    buffer1(1)=buffer1.getLong(1)+buffer2.getLong(1)
  }
//计算逻辑
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble/buffer.getLong(1)
  }
}
