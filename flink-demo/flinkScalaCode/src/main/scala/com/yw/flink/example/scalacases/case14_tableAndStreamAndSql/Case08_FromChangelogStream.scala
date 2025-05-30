package com.yw.flink.example.scalacases.case14_tableAndStreamAndSql

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Schema, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.types._

/**
  * Flink DataStream转换成Table - tableEnvironment.fromChangelogStream(...)
  */
object Case08_FromChangelogStream {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val ds = env.fromElements(
      Row.ofKind(RowKind.INSERT, "zs", Int.box(18)),
      Row.ofKind(RowKind.INSERT, "ls", Int.box(19)),
      Row.ofKind(RowKind.INSERT, "ww", Int.box(20)),
      Row.ofKind(RowKind.UPDATE_BEFORE, "zs", Int.box(18)),
      Row.ofKind(RowKind.UPDATE_AFTER, "zs", Int.box(21)),
      Row.ofKind(RowKind.DELETE, "ls", Int.box(19)),
      Row.ofKind(RowKind.INSERT, "xx", Int.box(108)),
    )(Types.ROW(Types.STRING, Types.INT))

    val result: Table = tableEnv.fromChangelogStream(
      ds,
      Schema.newBuilder()
        .primaryKey("f0")
        .build(),
      ChangelogMode.all()
    )

    result.printSchema();
    result.execute().print();


  }

}
