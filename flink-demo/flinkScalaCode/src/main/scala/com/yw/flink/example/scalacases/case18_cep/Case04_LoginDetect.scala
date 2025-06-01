package com.yw.flink.example.scalacases.case18_cep

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.time.Duration
import java.util

/**
  * Flink CEP - 用户恶意登录检测
  * 案例：读取socket用户登录数据，如果一个用户在20s内连续登录失败3次，那么输出报警信息
  */
object Case04_LoginDetect {
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
    val pattern: Pattern[LoginInfo, LoginInfo] =
      Pattern.begin[LoginInfo]("first").where(_.loginState.startsWith("fail"))
        .times(3).within(Time.seconds(20))

    //3.将规则应用到事件流中
    val patternStream: PatternStream[LoginInfo] = CEP.pattern(ds, pattern)

    //4.获取匹配数据
    val result: DataStream[String] = patternStream.process(new PatternProcessFunction[LoginInfo, String] {
      override def processMatch(
                                 map: util.Map[String, util.List[LoginInfo]],
                                 context: PatternProcessFunction.Context,
                                 collector: Collector[String]): Unit = {
        val first: util.List[LoginInfo] = map.get("first")
        val uid: String = first.get(0).uid


        collector.collect(s"用户：$uid ,连续3次登录失败！")

      }
    })

    result.print()
    env.execute()
  }

}
