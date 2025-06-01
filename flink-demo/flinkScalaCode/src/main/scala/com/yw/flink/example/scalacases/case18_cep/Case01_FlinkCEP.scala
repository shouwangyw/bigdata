package com.yw.flink.example.scalacases.case18_cep

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import java.time.Duration
import java.util

/**
  * Flink CEP Test
  * 案例：读取基站日志通话数据，当一个基站通话状态连续3次失败，就报警。
  */
object Case01_FlinkCEP {
  def main(args: Array[String]): Unit = {
    //1.准备事件流
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.streaming.api.scala._

    val keyedStream: KeyedStream[StationLog, String] = env.socketTextStream("nc_server", 9999)
      .map(line => {
        val arr: Array[String] = line.split(",")
        StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
      }).assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[StationLog](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[StationLog] {
          override def extractTimestamp(t: StationLog, l: Long): Long = t.callTime
        })
        .withIdleness(Duration.ofSeconds(5))
    ).keyBy(_.sid)


    //2.定义模式匹配规则
    val pattern: Pattern[StationLog, StationLog] =
      Pattern.begin[StationLog]("first").where(_.callType.equals("fail"))
        .next("second").where(_.callType.equals("fail"))
        .next("third").where(_.callType.equals("fail"))

    //3.匹配规则应用到事件流上
    val patternStream: PatternStream[StationLog] = CEP.pattern[StationLog](keyedStream, pattern)

    //4.获取匹配的数据
    val result: DataStream[String] = patternStream.process(new PatternProcessFunction[StationLog, String] {
      override def processMatch(
                                 map: util.Map[String, util.List[StationLog]],
                                 context: PatternProcessFunction.Context,
                                 collector: Collector[String]): Unit = {
        val first: StationLog = map.get("first").iterator().next()
        val second: StationLog = map.get("second").iterator().next()
        val third: StationLog = map.get("third").iterator().next()
        collector.collect(s"预警信息：$first \n $second \n $third")

      }
    })

    result.print()
    env.execute()
  }

}
