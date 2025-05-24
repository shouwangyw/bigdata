package com.yw.flink.example.scalacases.case01_source

import com.yw.flink.example.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Scala Flink 集合Source
  */
object Case02_CollectionSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._
    val logs: List[StationLog] = List[StationLog](
      StationLog("001", "186", "187", "busy", 1000L, 0L),
      StationLog("002", "187", "186", "fail", 1000L, 0L),
      StationLog("003", "188", "189", "success", 1000L, 0L),
      StationLog("004", "189", "187", "fail", 1000L, 0L),
      StationLog("005", "187", "186", "busy", 1000L, 0L)
    )
    val result: DataStream[StationLog] = env.fromCollection(logs)
    result.print()
    env.execute()
  }
}
