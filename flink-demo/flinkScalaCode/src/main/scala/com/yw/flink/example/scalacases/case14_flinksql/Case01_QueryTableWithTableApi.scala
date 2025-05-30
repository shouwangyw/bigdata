package com.yw.flink.example.scalacases.case14_flinksql

import org.apache.flink.table.api._

/**
  * 使用Table API 查询表
  * 案例：通过读取Kafka中基站日志数据，进行分析
  */
object Case01_QueryTableWithTableApi {
  def main(args: Array[String]): Unit = {
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .build()
    val tableEnv: TableEnvironment = TableEnvironment.create(settings)

    //创建Source 表
    tableEnv.createTable("station_log_tbl",
      TableDescriptor.forConnector("kafka")
        .schema(Schema.newBuilder()
          .column("sid", DataTypes.STRING())
          .column("call_out", DataTypes.STRING())
          .column("call_in", DataTypes.STRING())
          .column("call_type", DataTypes.STRING())
          .column("call_time", DataTypes.BIGINT())
          .column("duration", DataTypes.BIGINT())
          .build())
        .option("topic", "station_log-topic")
        .option("properties.bootstrap.servers", "node1:9092,node2:9092,node3:9092")
        .option("properties.group.id", "testgroup")
        .option("scan.startup.mode", "latest-offset")
        .option("format", "csv")
        .build()
    )

    val table: Table = tableEnv.from("station_log_tbl")
    val result: Table = table.filter($"call_type".isEqual("success").and($"duration".isGreater(10)))
      .groupBy($"sid")
      .select($"sid", $"duration".sum().as("total_duration"))

    result.execute().print()
  }
}
