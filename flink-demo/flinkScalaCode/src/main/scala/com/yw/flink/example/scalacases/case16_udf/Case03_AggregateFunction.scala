package com.yw.flink.example.scalacases.case16_udf

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.functions.AggregateFunction

import java.lang.{Double => JDouble, Integer => JInteger, Long => JLong}

/**
  * Flink Table API & SQL - 自定义聚合函数
  * 案例：读取Kafka 基站日志数据形成表 ，自定义聚合函数计算每个基站的平均通话的时长
  */
object AggregateFunctionTest {
  def main(args: Array[String]): Unit = {
    val tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build())
    //设置watermark 自动推进
    tableEnv.getConfig().set("table.exec.source.idle-timeout", "5000")

    tableEnv.executeSql("create table station_log_tbl(" +
      "   sid string," +
      "   call_out string," +
      "   call_in string," +
      "   call_type string," +
      "   call_time bigint," +
      "   duration bigint," +
      "   rowtime as to_timestamp_ltz(call_time,3)," +
      "   watermark for rowtime as rowtime - interval '2' seconds " +
      ") with (" +
      "   'connector'='kafka'," +
      "   'topic'='stationlog-topic'," +
      "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
      "   'properties.group.id'='testgroup'," +
      "   'scan.startup.mode'='latest-offset'," +
      "   'format'='csv'" +
      ")")

    //注册自定义聚合函数
    tableEnv.createTemporarySystemFunction("my_avg", classOf[AvgDurationUDAF])

    //使用Table API 调用聚合函数
    //    val result = tableEnv.from("station_log_tbl")
    //            .groupBy($("sid"))
    //            .select($("sid"), call("my_avg", $("duration")).as("avg_dur"))

    //    result.execute().print()

    //使用SQL 调用聚合函数
    tableEnv.executeSql("select sid,my_avg(duration) as avg_dur from station_log_tbl group by sid").print()
  }

}

class AvgDurationUDAF extends AggregateFunction[JDouble, JTuple2[JLong, JInteger]] {

  //初始化累加器
  override def createAccumulator(): JTuple2[JLong, JInteger] = JTuple2.of(0L, 0)

  /**
    * 计算聚合过程，计算逻辑
    */
  def accumulate(acc: JTuple2[JLong, JInteger], duration: JLong): Unit = {
    acc.f0 += duration
    acc.f1 += 1
  }

  //返回最终的聚合结果
  override def getValue(acc: JTuple2[JLong, JInteger]): JDouble = {
    if (acc.f1 == 0) {
      null

    } else {
      acc.f0 * 1.0 / acc.f1
    }

  }
}
