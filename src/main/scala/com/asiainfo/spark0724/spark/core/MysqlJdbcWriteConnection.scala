package com.asiainfo.spark0724.spark.core

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object MysqlJdbcWriteConnection {

  //向mysql数据库中写入数据
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("mysql").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    //从mysql中读取数据
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://s102:3306/test"
    val userName = "root"
    val passWd = "000000"
    var sql="select * from staff where id>=? and id<=?"
   val dataRDD: RDD[(Int, String, String)] = sc.makeRDD(Array((9,"liuyidao","male"),(10,"qiuxiang","female"),(11,"lirui","male")))

    //插入mysql数据库
    //存在一个问题，每循环一次都要建立一次连接，太浪费时间，不可能这么做
//    dataRDD.foreach{
//      case(id,name,sex)=>{
//        Class.forName(driver)
//        val conn: Connection = DriverManager.getConnection(url,userName,passWd)
//        val statement: PreparedStatement = conn.prepareStatement("insert into test1(id,name,sex) values(?,?,?)")
//        statement.setInt(1,id)
//        statement.setString(2,name)
//        statement.setString(3,sex)
//        statement.executeUpdate()
//        //关闭连接信息
//        statement.close()
//        conn.close()
//
//      }
//    }
    //考虑把连接信息放到外面，这样就只建立一次连接了
    // Task not serializable ,报错，不能序列化，原因是数据库的连接信息是不能序列化传输的
    // 算子之外的代码是在diver进行，算子内的代码是在executor端执行，所以必须要进行序列化
//    Class.forName(driver)
//    val conn: Connection = DriverManager.getConnection(url,userName,passWd)
//    dataRDD.foreach{
//          case(id,name,sex)=>{
//        val statement: PreparedStatement = conn.prepareStatement("insert into test1(id,name,sex) values(?,?,?)")
//        statement.setInt(1,id)
//        statement.setString(2,name)
//        statement.setString(3,sex)
//        statement.executeUpdate()
//        //关闭连接信息
//        statement.close()
//
//      }
//    }
//    conn.close()

    //目的是减少连接数，可以按照分区来做
    dataRDD.foreachPartition(
      iter=>{
        Class.forName(driver)
        val conn: Connection = DriverManager.getConnection(url,userName,passWd)
        val st: PreparedStatement = conn.prepareStatement("insert into test1(id,name,sex) values(?,?,?)")
        iter.foreach{
          case (id,name,sex)=>{
            st.setInt(1,id)
            st.setString(2,name)
            st.setString(3,sex)
            st.executeUpdate()

          }
        }
        st.close()
        conn.close()

      }

    )

    //释放sc
    sc.stop
  }
}
