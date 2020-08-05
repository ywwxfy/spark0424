package com.asiainfo.spark0724.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object SparkSQL04_Hive {

  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL04_Hive")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    import spark.implicits._

    // 创建聚合函数
    val calcCitydata = new CalcCitydata;

    spark.udf.register("calcCitydata", calcCitydata)

    // TODO 读取Hive中表的数据
    spark.sql("use sparkpractice190513")

    spark.sql(
      """
        |            select
        |                v.*,
        |                p.product_name,
        |                c.city_name,
        |                c.area
        |            from user_visit_action v
        |            join product_info p on v.click_product_id = p.product_id
        |            join city_info c on v.city_id = c.city_id
        |            where v.click_product_id > -1
      """.stripMargin).createOrReplaceTempView("t1")

    spark.sql(
      """
        |    select
        |        t1.area,
        |        t1.product_name,
        |        count(*) as areaProductClick,
        |        calcCitydata(t1.city_name)
        |    from t1
        |    group by t1.area, t1.product_name
      """.stripMargin).createOrReplaceTempView("t2")

    spark.sql(
      """
        |select
        |   *
        |from (
        |    select
        |        *,
        |        rank() over( partition by t2.area order by t2.areaProductClick desc ) as rank
        |    from t2
        |) t3
        |where rank < 3
      """.stripMargin).show(20)

    // TODO 释放资源
    spark.stop()

  }
}
// 声明聚合函数
// 1. 继承UserDefinedAggregateFunction
// 2. 重写方法
class CalcCitydata extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = {
    StructType(Array(StructField("cityName", StringType)))
  }

  // total, (北京 - 100，天津-50)
  override def bufferSchema: StructType = {
    StructType(Array(StructField("cityToCount", MapType(StringType, LongType)), StructField("total", LongType)))
  }

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, Long]()
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val cityName = input.getString(0)
    val map: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    buffer(0) = map + (cityName -> (map.getOrElse(cityName, 0L) + 1L))
    buffer(1) = buffer.getLong(1) + 1L
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    val map1: Map[String, Long] = buffer1.getAs[Map[String, Long]](0)
    val map2: Map[String, Long] = buffer2.getAs[Map[String, Long]](0)

    buffer1(0) = map1.foldLeft(map2){
      case ( map, (k, v) ) => {
        map + (k -> (map.getOrElse(k, 0L) + v))
      }
    }
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = {

    // 获取城市点击次数，并根据点击次数进行排序取2条
    val map: Map[String, Long] = buffer.getAs[Map[String, Long]](0)

    val remarkList: List[(String, Long)] = map.toList.sortWith(
      (left, right) => {
        left._2 > right._2
      }
    )

    if ( remarkList.size > 2 ) {

      val restList: List[(String, Long)] = remarkList.take(2)
      val cityList: List[String] = restList.map {
        case (cityName, clickCount) => {
          cityName + clickCount.toDouble / buffer.getLong(1) * 100 + "%"
        }
      }
      cityList.mkString(", ") + ", 其他 " + ( remarkList.tail.tail.map(_._2).sum / buffer.getLong(1) * 100 + "%" )

    } else {
      val cityList: List[String] = remarkList.map {
        case (cityName, clickCount) => {
          cityName + clickCount.toDouble / buffer.getLong(1) * 100 + "%"
        }
      }
      cityList.mkString(", ")
    }


  }
}
