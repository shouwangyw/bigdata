package com.yw.flink.example.scalacases.case03_richfunc

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat

/**
 * Scala - Flink 富函数类
 */
object RichFunTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val ds: DataStream[String] = env.socketTextStream("nc_server", 9999)
    val result: DataStream[String] = ds.map(new MyRichMapFun())
    result.print()
    env.execute()

  }

}
class MyRichMapFun() extends RichMapFunction[String,String]{
  var conn: Connection = _
  var pst: PreparedStatement = _
  var rst: ResultSet = _

  //用于初始化资源，在map方法执行前调用一次
  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://node2:3306/mydb?useSSL=false", "root", "123456")
    pst = conn.prepareStatement("select phone_num,name,city from person_info where phone_num = ?")

  }

  //有一条数据就会调用一次
  override def map(line: String): String = {
    val split: Array[String] = line.split(",")
    val sid: String = split(0)
    val callOut: String = split(1)
    val callIn: String = split(2)
    val callType: String = split(3)
    val callTime: Long = split(4).toLong
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time: String = sdf.format(callTime)
    val duration: Long = split(5).toLong

    var callOutName: String = ""
    var callInName: String = ""

    pst.setString(1,callOut)
    rst = pst.executeQuery()
    while(rst.next()){
      callOutName = rst.getString("name")
    }
    pst.setString(1,callIn)
    rst = pst.executeQuery()
    while(rst.next()){
      callInName = rst.getString("name")
    }

    "基站：" + sid + "，主叫：" + callOutName + "，被叫：" + callInName + "，呼叫类型：" + callType + "，呼叫时间" + time + "，呼叫时长" + duration

  }


  //在flink程序停止的时候调用
  override def close(): Unit = {
    rst.close()
    pst.close()
    conn.close()

  }
}