package com.yw.flink.example.scalacases.case14_tableAndStreamAndSql

import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, FormatDescriptor, Schema, Table, TableDescriptor, TableEnvironment}

/**
  * 使用Table API 查询表
  * 案例：通过读取Kafka中基站日志数据，进行分析
  */
object Case04_TableSinkWithTableApi {
  def main(args: Array[String]): Unit = {
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .build()
    val tableEnv: TableEnvironment = TableEnvironment.create(settings)

    tableEnv.getConfig.getConfiguration.setLong("execution.checkpointing.interval", 5000L)

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
        .option("topic", "stationlog-topic")
        .option("properties.bootstrap.servers", "node1:9092,node2:9092,node3:9092")
        .option("properties.group.id", "testgroup")
        .option("scan.startup.mode", "latest-offset")
        .option("format", "csv")
        .build()
    )

    val result: Table = tableEnv.from("station_log_tbl")

    //3.创建 Filesystem Connector 表
    tableEnv.createTemporaryTable("CsvSinkTable",
      TableDescriptor.forConnector("filesystem")
        .schema(Schema.newBuilder()
          .column("sid", DataTypes.STRING())
          .column("call_out", DataTypes.STRING())
          .column("call_in", DataTypes.STRING())
          .column("call_type", DataTypes.STRING())
          .column("call_time", DataTypes.BIGINT())
          .column("duration", DataTypes.BIGINT())
          .build())
        .option("path", ".data/flink/output")
        .option("sink.rolling-policy.check-interval", "2s")
        .option("sink.rolling-policy.rollover-interval", "10s")
        .format(FormatDescriptor.forFormat("csv")
          .option("field-delimiter", "|")
          .build())
        .build()

    )

    //Table API 写出数据
    //        result.insertInto("CsvSinkTable").execute()
    result.executeInsert("CsvSinkTable")

  }

}
