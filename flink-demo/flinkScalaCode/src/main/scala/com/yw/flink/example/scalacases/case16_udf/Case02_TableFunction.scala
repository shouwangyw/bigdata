package com.yw.flink.example.scalacases.case16_udf

import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/**
  * Flink Table API & SQL - 自定义表函数
  * 案例：读取Kafka 数据，对数据使用自定义表函数进行查询
  */
object TableFunctionTest {
  def main(args: Array[String]): Unit = {
    val tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build())
    //设置watermark 自动推进
    tableEnv.getConfig().set("table.exec.source.idle-timeout", "5000")

    tableEnv.executeSql("create table station_log_tbl(" +
      "   id int," +
      "   strs string," +
      "   dt bigint," +
      "   rowtime as to_timestamp_ltz(dt,3)," +
      "   watermark for rowtime as rowtime - interval '2' seconds " +
      ") with (" +
      "   'connector'='kafka'," +
      "   'topic'='stationlog-topic'," +
      "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
      "   'properties.group.id'='testgroup'," +
      "   'scan.startup.mode'='latest-offset'," +
      "   'format'='csv'" +
      ")")

    //注册表函数
    tableEnv.createTemporarySystemFunction("my_split", classOf[SplitStringUDTF]);

    //Table API 使用自定义表函数
    //    val result = tableEnv.from("station_log_tbl")
    //      .joinLateral(call("my_split", $"strs").as("str", "len"))
    //      .select($"id", $"str", $"len")
    //    result.execute().print()
    //    //SQL 方式调用自定义表函数
    tableEnv.executeSql("select id,str,len" +
      " from station_log_tbl , lateral table (my_split(strs)) as T(str,len)").print()

  }

}

@FunctionHint(output = new DataTypeHint("ROW<word STRING, length INT>"))
class SplitStringUDTF extends TableFunction[Row] {
  def eval(strs: String) = {
    strs.split("\\|").foreach(s => {
      collect(Row.of(s, Int.box(s.length)))
    })

  }

}
