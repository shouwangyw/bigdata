package com.yw.flink.example.scalacases.case14_tableAndStreamAndSql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api._

/**
  * Flink Table API - FullOuterJoin
  * 案例：读取Socket - 8888 数据 形成表 与 读取Socket -9999 数据形成表进行 FullOuterJoin关联
  * socket-8888 : 1,zs,18,1000
  * scoket-9999 : 1,zs,beijing,1000
  */
object Case17_FullOuterJoin {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //读取socket-8888: 1,zs,18,1000
    val personDS: DataStream[(Int, String, Int, Long)] = env.socketTextStream("node5", 8888)
      .map(line => {
        val arr: Array[String] = line.split(",")
        (arr(0).toInt, arr(1), arr(2).toInt, arr(3).toLong)
      })

    val addressDS: DataStream[(Int, String, String, Long)] = env.socketTextStream("node5", 9999)
      .map(line => {
        val arr: Array[String] = line.split(",")
        (arr(0).toInt, arr(1), arr(2), arr(3).toLong)
      })

    //将DS转换成Table
    val personTable: Table = tableEnv.fromDataStream(personDS,
      Schema.newBuilder()
        .column("_1", DataTypes.INT())
        .column("_2", DataTypes.STRING())
        .column("_3", DataTypes.INT())
        .column("_4", DataTypes.BIGINT())
        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(_4,3)")
        .watermark("rowtime", "rowtime - INTERVAL '2' SECONDS")
        .build()
    ).as("left_id", "left_name", "age", "left_dt", "left_rowtime")

    val addressTable: Table = tableEnv.fromDataStream(addressDS,
      Schema.newBuilder()
        .column("_1", DataTypes.INT())
        .column("_2", DataTypes.STRING())
        .column("_3", DataTypes.STRING())
        .column("_4", DataTypes.BIGINT())
        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(_4,3)")
        .watermark("rowtime", "rowtime - INTERVAL '2' SECONDS")
        .build()
    ).as("right_id", "right_name", "city", "right_dt", "right_rowtime")


    personTable.printSchema()
    addressTable.printSchema()

    //fulloutJoin
    val result: Table = personTable.fullOuterJoin(addressTable, $"left_id" === $"right_id")
      .select(
        $"left_id",
        $"left_name",
        $"age",
        $"right_id",
        $"right_name",
        $"city"
      )
    result.execute().print()
  }
}
