package com.asiainfo.spark0724.sparksql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable

//   需求 ：最终期望得到的结果
//    地区	商品名称	点击次数	城市备注
//        华北	商品A	100000	北京21.2%，天津13.2%，其他65.6%
//        华北	商品P	80200	北京63.0%，太原10%，其他27.0%
//        华北	商品M	40000	北京63.0%，太原10%，其他27.0%
//        东北	商品J	92000	大连28%，辽宁17.0%，其他 55.0%
object SparkSQL_Top3 {

  //spark session 加载数据源
  def main(args: Array[String]): Unit = {
    //启用对hive 的支持
    val spark: SparkSession = SparkSession.builder()
//      .config("spark.sql.warehouse.dir", "hdfs://s102:8020/user/hive/warehouse")
      .master("local[*]")
      .appName("hive")
      .enableHiveSupport()
      .getOrCreate()

  import spark.implicits._

    val city: UserDefinedAggregateFunction = spark.udf.register("city_remark",new City_Remark)
    //访问hive的库，是通过hive-site.xml来访问hive库的
    spark.sql("use sparkpractice")
    //TODO 拿到所有城市的点击的数据
   spark.sql(
      """
        |select
        |
        |        uv.*,
        |        area,
        |        city_name,
        |        product_name
        |    from user_visit_action uv
        |    join city_info cf on uv.city_id=cf.city_id
        |    join product_info pf on uv.click_product_id=pf.product_id
        |    where uv.click_product_id>-1
      """.stripMargin).createOrReplaceTempView("t1")
          //按照地区和产品名称分组，把一个地区的所有城市聚合到一起
    spark.sql(
      """
        |select
        |    area,
        |    product_name,
        |    count(*) clickCount,
        |    city_remark(city_name) remark
        |
        |from t1
        |group by t1.area,t1.product_name
      """.stripMargin).createOrReplaceTempView("t2")
    spark.sql(
      """
        |select area,
        | product_name,
        | clickCount,
        | remark,
        | rank
        |from (
        | select *,
        |   rank() over(partition by t2.area order by t2.clickCount desc ) rank
        |from t2 ) t3
        |where rank<=3
      """.stripMargin).show()



    //TODO 释放对象
    spark.stop()
  }



}

//自定义一个聚合函数,不需要强类型的
class City_Remark extends UserDefinedAggregateFunction{
  //输入结构
  override def inputSchema: StructType = StructType(Array(StructField("city_name",StringType)))

  //缓冲区中是一个map结构，要把每个城市都要存放起来，字段一个sum求和，key为城市名，value为出现的次数
  //按照地区分组的所有点击量 count
  override def bufferSchema: StructType = {
    StructType(Array(StructField("map",MapType(StringType,LongType)),StructField("count",LongType)))
  }
  //最后返回的结果类型
  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  //初始化map
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=Map[String,Long]()
    buffer(1)=0L

  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //更新自定义聚合函数的值，这里的操作有点难理解
    val cityName: String = input.getString(0)
    //buffer是复杂类型，需要转化为复杂类型
    val map: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
//    val hashmap = new mutable.HashMap[String,Long]()
//   val map2: mutable.Map[String, Long] = mutable.Map[String,Long]()
//    hashmap.update(k,v) 可变集合有update方法，map(key)=2L表示的是调用了update 方法
//    hashmap(cityName)=hashmap.getOrElse(cityName,0L)+1L
    //这里的不可变map 没有update方法，
//    map(cityName)= map.getOrElse(cityName,0L)+1L
    //不可变map可以增加key value，产生一个新的集合
    buffer(0)= map+(cityName -> (map.getOrElse(cityName,0L)+1L))
    buffer(1)=buffer.getLong(1)+1L
//    buffer(1)+1L
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //缓冲区的数据合并
    val map1: Map[String, Long] = buffer1.getAs[Map[String,Long]](0)
    val map2: Map[String, Long] = buffer2.getAs[Map[String,Long]](0)
    buffer1(0)= map1.foldLeft(map2) {
      case (map, kv) => {
        map + (kv._1 -> kv._2)
      }
    }
    buffer1(1)=buffer1.getLong(1)+buffer2.getLong(1)
  }

  //开始得出最后需要的值 北京-20%，天津-10%，其他-70%
  override def evaluate(buffer: Row): String = {
    val newMap: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
    val result: List[(String, Long)] = newMap.toList.sortWith(_._2>_._2)
    if(result.size<3){

      val strs: List[String] = result.map {
        case (key, sum) => key + " " + (sum.toDouble/ buffer.getLong(1)*100).toDouble.formatted("%.2f")+"%"
      }
      strs.mkString(",")
    }else {
      //如果本来就只有2个城市就不需要有其他选项了
      val firstAndSecond: List[(String, Long)] = result.take(2)
      val strings: List[String] = firstAndSecond.map {

        case (key, sum) => key + " " + (sum.toDouble / buffer.getLong(1)*100).toDouble.formatted("%.2f") + "%"
      }

      strings.mkString(",")+"，其他 "+(result.tail.tail.map(_._2).sum.toDouble/buffer.getLong(1)*100).toDouble.formatted("%.2f") +"%"
    }

  }
}
