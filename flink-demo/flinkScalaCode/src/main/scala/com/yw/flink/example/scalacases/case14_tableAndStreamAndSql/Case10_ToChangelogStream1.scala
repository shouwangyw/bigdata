package com.yw.flink.example.scalacases.case14_tableAndStreamAndSql

import com.yw.flink.example.StationLog
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{Schema, Table}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import java.time.Duration

/**
  * Flink Table 转换成DataStream - tableEnv.toChangelogStream(table)
  * 1.创建DataStream
  * 2.设置watermark
  * 3.dataStream -> Table (传递watermark)
  * 4.toChangelogStream:table -> DataStream ： 观察是否有watermark传递
  */
object Case10_ToChangelogStream1 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    import org.apache.flink.streaming.api.scala._

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //创建DataStream
    val stationLogDS: DataStream[StationLog] = env.socketTextStream("node5", 9999)
      .map(line => {
        val arr: Array[String] = line.split(",")
        StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
      })

    //设置watermark
    val dsWithWatermark: DataStream[StationLog] = stationLogDS.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[StationLog](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[StationLog] {
          override def extractTimestamp(t: StationLog, l: Long): Long = t.callTime
        })
    )

    //dataStream -> Table (传递watermark)
    val table: Table = tableEnv.fromDataStream(dsWithWatermark, Schema.newBuilder()
      .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(callTime,3)")
      .watermark("rowtime", "SOURCE_WATERMARK()")
      .build())

    //对table进行聚合
    val result: Table = table.groupBy($"sid")
      .select($"sid", $"duration".sum().as("total_duration"))

    //toChangelogStream:table -> DataStream ： 观察是否有watermark传递
    val ds: DataStream[Row] = tableEnv.toChangelogStream(result)

    ds.process(new ProcessFunction[Row, String] {
      override def processElement(in: Row,
                                  context: ProcessFunction[Row, String]#Context,
                                  collector: Collector[String]): Unit =
        collector.collect(s"数据：$in ，watermark:${context.timerService().currentWatermark()}")
    }).print()

    env.execute()

  }

}
