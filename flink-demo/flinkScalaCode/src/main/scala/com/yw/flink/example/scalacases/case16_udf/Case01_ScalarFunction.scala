package com.yw.flink.example.scalacases.case16_udf

import org.apache.flink.table.annotation.{DataTypeHint, InputGroup}
import org.apache.flink.table.api.Expressions.call
import org.apache.flink.table.api.{EnvironmentSettings, FieldExpression, Table, TableEnvironment}
import org.apache.flink.table.functions.ScalarFunction

import scala.annotation.varargs

/**
  * Flink Table API & SQL - 自定义函数 - Scalar Function
  * 案例：读取Kafka 中基站日志数据，通过自定义函数输出信息
  */
object ScalarFunctionTest {
  def main(args: Array[String]): Unit = {
    val tableEnv: TableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build())
    //设置watermark自动推进
    tableEnv.getConfig.set("table.exec.source.idle-timeout", "5000")

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

    //注册自定义标量函数
    tableEnv.createTemporarySystemFunction("my_concat", classOf[ConcatStringUDF])

    //SQL 方式调用自定义函数
    //    tableEnv.executeSql("select sid,my_concat(call_out,call_in,call_type,duration) as info from station_log_tbl").print()

    val result: Table = tableEnv.from("station_log_tbl")
      .select($"sid", call("my_concat", $"call_in", $"call_out", $"call_type"))
    result.execute().print()

  }

}

class ConcatStringUDF extends ScalarFunction {
  @varargs
  def eval(@DataTypeHint(inputGroup = InputGroup.ANY) args: AnyRef*): String = {
    args.map(f => {
      f.toString
    }).mkString("|")
  }

}

