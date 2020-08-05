package com.asiainfo.spark0724.sparksql


import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql._

object UDFAggrFuncStrong {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkSql")
    //Todo 创建 sparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val df: DataFrame = spark.read.json("input/1.json")
    import spark.implicits._
//    df.createTempView("people")
    val empDS: Dataset[Emp] = df.as[Emp]
val udfAvgAge = new UDFAggrFuncStrong
    //讲聚合函数转换为查询列
   val column: TypedColumn[Emp, Double] = udfAvgAge.toColumn
   empDS.select(column).show()


    //释放连接
    spark.stop()
  }


}
//样例类
case class Emp(username: String,age:Long)
//缓冲区数据处理样例类
case class AvgBuffer(var sum:Long,var count:Long)

//自定义一个求平均值的强类型函数
class UDFAggrFuncStrong extends Aggregator[Emp,AvgBuffer,Double]{
  //初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0L,0L)
  }
  //同一个 executor 的数据合并
  override def reduce(b: AvgBuffer, a: Emp): AvgBuffer = {
    b.sum=b.sum+a.age
    b.count=b.count+1L
    b
  }
//合并多个executor之间的数据
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum=b1.sum+b2.sum
    b1.count=b1.count+b2.count
    b1
  }
//最终结果计算逻辑
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble/reduction.count
  }
  //内置编码解码器
  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product
//要使用scala的double
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

