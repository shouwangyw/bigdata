package com.yw.flink.example.scalacases.case18_cep

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import java.time.Duration
import java.util

/**
 * Flink CEP - greedy 贪婪模式使用
 * 案例：读取socket用户登录数据，当用户登录成功后，输出用户登录成功前的所有登录状态
 */

case class LoginInfo(uid:String,userName:String,loginTime:Long,loginState:String)

object GreedyTest {
  def main(args: Array[String]): Unit = {
    //1.定义事件流
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    val ds: KeyedStream[LoginInfo, String] = env.socketTextStream("nc_server", 9999)
      .map(line => {
        val arr: Array[String] = line.split(",")
        LoginInfo(arr(0), arr(1), arr(2).toLong, arr(3))
      }).assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[LoginInfo](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[LoginInfo] {
          override def extractTimestamp(t: LoginInfo, l: Long): Long = t.loginTime
        }).withIdleness(Duration.ofSeconds(5))
    ).keyBy(_.uid)


    //2.定义模式匹配规则
    val pattern: Pattern[LoginInfo, LoginInfo] = Pattern.begin[LoginInfo]("first").where(_.loginState.startsWith("fail"))
      .oneOrMore
      .greedy
      .followedBy("second").where(_.loginState.equals("success"))

    //3.将规则应用到事件流中
    val patternStream: PatternStream[LoginInfo] = CEP.pattern(ds, pattern)

    //4.获取匹配数据
    val result: DataStream[String] = patternStream.process(new PatternProcessFunction[LoginInfo, String] {
      override def processMatch(
                                 map: util.Map[String, util.List[LoginInfo]],
                                 context: PatternProcessFunction.Context,
                                 collector: Collector[String]): Unit = {
        val first: util.List[LoginInfo] = map.get("first")
        val second: util.List[LoginInfo] = map.get("second")
        val uid: String = first.get(0).uid
        val successTime: Long = second.get(0).loginTime

        import scala.collection.JavaConverters._
        val info: String = first.asScala.map(_.loginState).mkString("-")

        collector.collect(s"用户：$uid ,在 $successTime 登录成功，登录前的状态：$info")

      }
    })

    result.print()
    env.execute()
  }

}
