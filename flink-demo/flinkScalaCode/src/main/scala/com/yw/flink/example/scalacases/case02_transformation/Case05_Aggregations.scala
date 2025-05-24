package com.yw.flink.example.scalacases.case02_transformation

import com.yw.flink.example.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  *
  * @author yangwei
  */
object Case05_Aggregations {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val ds: DataStream[StationLog] = env.fromCollection(List(
      StationLog("sid1", "18600000000", "18600000001", "success", System.currentTimeMillis(), 120L),
      StationLog("sid1", "18600000001", "18600000002", "fail", System.currentTimeMillis(), 30L),
      StationLog("sid1", "18600000002", "18600000003", "busy", System.currentTimeMillis(), 50L),
      StationLog("sid1", "18600000003", "18600000004", "barring", System.currentTimeMillis(), 90L),
      StationLog("sid1", "18600000004", "18600000005", "success", System.currentTimeMillis(), 300L)
    ))

    val ds2: KeyedStream[StationLog, String] = ds.keyBy(_.sid)
    //    ds2.sum("duration")
    val result1: DataStream[StationLog] = ds2.min("duration")
    result1.print("result1")
    val result2: DataStream[StationLog] = ds2.minBy("duration")
    result2.print("result2")

    env.execute()
  }
}
