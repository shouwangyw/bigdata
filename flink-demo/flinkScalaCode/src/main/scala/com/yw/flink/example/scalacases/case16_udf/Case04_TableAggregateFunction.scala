package com.yw.flink.example.scalacases.case16_udf

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.table.api.Expressions.call
import org.apache.flink.table.api.{$, EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.util.Collector

import java.lang.{Integer => JInteger, Long => JLong}

/**
  * Flink Table API - 表聚合函数
  * 案例：读取Kafka 基站日志数据，对每个基站输出通话时长top2
  */
object TableAggregateFunctionTest {
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

    //注册自定义表聚合函数
    tableEnv.createTemporarySystemFunction("my_top2Agg", classOf[Top2DurationTableUDAF])

    //表聚合函数只能通过Table api 调用
    val result = tableEnv.from("station_log_tbl")
      .groupBy($("sid"))
      .flatAggregate(call("my_top2Agg", $("duration")).as("dur", "rank"))
      .select($("sid"), $("dur"), $("rank"))

    result.execute().print()
  }

}

/**
  * T:聚合后结果
  * ACC:累加器类型
  */
class Top2DurationTableUDAF extends TableAggregateFunction[JTuple2[JLong, JInteger], JTuple2[JLong, JLong]] {
  //初始化累加器
  override def createAccumulator(): JTuple2[JLong, JLong] = JTuple2.of(JLong.MIN_VALUE, JLong.MIN_VALUE)

  /**
    * 计算逻辑
    */
  def accumulate(acc: JTuple2[JLong, JLong], duration: JLong): Unit = {
    if (duration > acc.f0) {
      acc.f1 = acc.f0;
      acc.f0 = duration;
    } else if (duration > acc.f1) {
      acc.f1 = duration;

    }

  }

  /**
    * 返回结果
    */
  def emitValue(acc: JTuple2[JLong, JLong], out: Collector[JTuple2[JLong, JInteger]]): Unit = {
    if (acc.f0 != JLong.MIN_VALUE) {
      out.collect(JTuple2.of(acc.f0, 1))
    }

    if (acc.f1 != JLong.MIN_VALUE) {
      out.collect(JTuple2.of(acc.f1, 2))
    }
  }

}
