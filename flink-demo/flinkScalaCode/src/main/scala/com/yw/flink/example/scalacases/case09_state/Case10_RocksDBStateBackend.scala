package com.yw.flink.example.scalacases.case09_state

import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Flink RocksDBStateBackend状态测试
 *
 */
object Case10_RocksDBStateBackend {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    import org.apache.flink.streaming.api.scala._

    env.getCheckpointConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    env.socketTextStream("nc_server",9999)
      .flatMap(_.split(","))
      .map((_,1))
      .keyBy(_._1)
      .sum(1)
      .print()
    env.execute()
  }

}
