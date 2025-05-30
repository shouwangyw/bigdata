package com.yw.flink.example.scalacases.case14_flinksql

import com.yw.flink.example.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{Schema, Table}

/**
  * Flink DataStream转换成Table - tableEnvironment.fromDataStream(dataStream,Schema)
  */
object Case06_FromDataStream2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val ds: DataStream[StationLog] = env.socketTextStream("node5", 9999)
      .map(line => {
        val arr: Array[String] = line.split(",")
        StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
      })

    val result: Table = tableEnv.fromDataStream(ds,
      Schema.newBuilder()
        .columnByExpression("proc_time", "PROCTIME()")
        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(callTime,3)")
        .watermark("rowtime", "rowtime - INTERVAL '2' Seconds")
        .build())

    result.printSchema()
    result.execute().print()
  }

}
