package com.yw.flink.example.scalacases.case09_state

import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


/**
 * Checkpoint状态恢复测试
 * 案例：读取socket中的数据，统计wc
 */
object Case09_CheckpointRecover {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    import org.apache.flink.streaming.api.scala._

    env.getCheckpointConfig.setCheckpointStorage("hdfs://mycluster/flink-checkpoints")
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
