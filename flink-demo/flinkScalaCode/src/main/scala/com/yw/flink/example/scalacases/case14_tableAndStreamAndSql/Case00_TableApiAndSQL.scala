package com.yw.flink.example.scalacases.case14_tableAndStreamAndSql

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Schema, Table, TableDescriptor, TableEnvironment}

/**
  * Flink Table API 和SQL 快速上手
  */
object Case00_TableApiAndSQL {
  def main(args: Array[String]): Unit = {
    //1.创建环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .build()
    val tableEnv: TableEnvironment = TableEnvironment.create(settings)

    //2.设置参数
    tableEnv.getConfig.getConfiguration.setString("parallelism.default", "1")

    //3.通过Table API 创建Source 表
    tableEnv.createTemporaryTable("SourceTable",
      TableDescriptor.forConnector("datagen")
        .schema(Schema.newBuilder()
          .column("f0", DataTypes.STRING())
          .build())
        .option[java.lang.Long](DataGenConnectorOptions.ROWS_PER_SECOND, 10L)
        .build()
    )

    //4.通过SQL DDL创建Sink Table
    tableEnv.executeSql(
      """
        |create table SinkTable(
        | f0 string
        |) with (
        | 'connector' = 'print'
        |)
        |""".stripMargin)
    //    tableEnv.executeSql("" +
    //      "create table SinkTable(" +
    //      " f0 string" +
    //      ") with (" +
    //      " 'connector'='print'" +
    //      ")")

    //5.通过Table API 查询Source 表
    val table1: Table = tableEnv.from("SourceTable")
    //    table.printSchema()
    //    table.execute().print()

    //6.通过SQL 方式查询Source 表
    val table2: Table = tableEnv.sqlQuery("select * from SourceTable")

    //7.通过Table API 将结果写出到Sink
    table1.executeInsert("SinkTable")

    //8.通过SQL 方式将结果写出到Sink
    //    tableEnv.executeSql("insert into SinkTable select * from SourceTable")

  }

}
