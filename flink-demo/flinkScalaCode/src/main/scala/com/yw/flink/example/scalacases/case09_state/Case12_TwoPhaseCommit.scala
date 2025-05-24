package com.yw.flink.example.scalacases.case09_state

import com.yw.flink.example.JdbcCommonUtils
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeutils.base.VoidSerializer
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, TwoPhaseCommitSinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.sql.PreparedStatement

/**
 * 案例：读取Kafka中数据，自己实现两阶段提交方式写出到MySQL中
 */
object TwoPhaseCommitTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._
    env.enableCheckpointing(5000)
//    env.setParallelism(1)

    /**
     * 读取Kafka中数据
     */
    val kafkaSource: KafkaSource[String] = KafkaSource.builder()
      .setBootstrapServers("node1:9092,node2:9092,node3:9092")
      .setTopics("2pc-topic")
      .setGroupId("my-test-group")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val kafkaDS: DataStream[String] = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")
    //将KafkaDS写出到mysql中
    kafkaDS.addSink(new CustomTwoPhaseCommitSinkFunction())
    env.execute()

  }
}

class CustomTwoPhaseCommitSinkFunction extends TwoPhaseCommitSinkFunction[String,JdbcCommonUtils,Void](
  new KryoSerializer[JdbcCommonUtils](classOf[JdbcCommonUtils],new ExecutionConfig()), VoidSerializer.INSTANCE
){


  override def beginTransaction(): JdbcCommonUtils = {
    println("beginTransaction...")
    new  JdbcCommonUtils
  }

  override def invoke(jdbcCommonUtils: JdbcCommonUtils, value: String, context: SinkFunction.Context): Unit = {
    val split: Array[String] = value.split(",")
    val pst: PreparedStatement = jdbcCommonUtils.getConnect.prepareStatement("insert into user(id,name,age) values (?,?,?)")
    pst.setInt(1,split(0).toInt)
    pst.setString(2,split(1))
    pst.setInt(3,split(2).toInt)

    pst.execute()

    pst.close()

  }

  override def preCommit(jdbcCommonUtils: JdbcCommonUtils): Unit = {
    println("barrier到达，开启新的事务")
    //....
  }

  /**
   * checkpoint完成，jobmanger通知ck完成回调方法中会调用该方法
   */
  override def commit(jdbcCommonUtils: JdbcCommonUtils): Unit = {
    println("commit .... ")
    jdbcCommonUtils.commit()


  }

  /**
   * Flink代码出现问题会调用该方法，这里回滚事务
   */
  override def abort(jdbcCommonUtils: JdbcCommonUtils): Unit = {
    println("abort...")
    jdbcCommonUtils.rollback()
    jdbcCommonUtils.close()


  }
}
