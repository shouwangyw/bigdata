package com.yw.flink.example.scalacases.case14_tableAndStreamAndSql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api._

/**
  * Flink Table API - Interval Join
  * 案例：读取用户登录流和用户点击广告流形成表，进行IntervalJoin
  */
object Case18_IntervalJoin {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //读取socket-8888: user_1,6000
    val loginDS: DataStream[(String, Long)] = env.socketTextStream("node5", 8888)
      .map(line => {
        val arr: Array[String] = line.split(",")
        (arr(0), arr(1).toLong)
      })

    //user_1,product_1,3000
    val clickDS: DataStream[(String, String, Long)] = env.socketTextStream("node5", 9999)
      .map(line => {
        val arr: Array[String] = line.split(",")
        (arr(0), arr(1), arr(2).toLong)
      })

    //将DS转换成Table
    val loginTable: Table = tableEnv.fromDataStream(loginDS,
      Schema.newBuilder()
        .column("_1", DataTypes.STRING())
        .column("_2", DataTypes.BIGINT())
        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(_2,3)")
        .watermark("rowtime", "rowtime - INTERVAL '2' SECONDS")
        .build()
    ).as("left_uid", "left_dt", "left_rowtime")

    val clickTable: Table = tableEnv.fromDataStream(clickDS,
      Schema.newBuilder()
        .column("_1", DataTypes.STRING())
        .column("_2", DataTypes.STRING())
        .column("_3", DataTypes.BIGINT())
        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(_3,3)")
        .watermark("rowtime", "rowtime - INTERVAL '2' SECONDS")
        .build()
    ).as("right_uid", "right_product", "right_dt", "right_rowtime")

    //interval Join
    val result: Table = loginTable.join(clickTable)
      .where(
        $"left_uid" === $"right_uid" &&
          $"right_rowtime" >= $"left_rowtime" - 2.seconds() &&
          $"right_rowtime" < $"left_rowtime" + 2.seconds()

      ).select($"left_uid", $"left_dt", $"right_uid", $"right_dt", $"right_product")

    result.execute().print()
  }
}
