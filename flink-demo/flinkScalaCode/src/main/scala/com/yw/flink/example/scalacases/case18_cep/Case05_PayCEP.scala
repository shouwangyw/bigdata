package com.yw.flink.example.scalacases.case18_cep

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.functions.{PatternProcessFunction, TimedOutPartialMatchHandler}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.time.Duration
import java.util

/**
 * Flink CEP - 订单支付超时检测
 * 案例：读取Socket基站用户订单数据，
 * 如果用户在下订单后的一定时间内进行了支付，提示订单支付成功发货。 - 主流
 * 如果用户在下订单后的一定时间内没有支付，提示订单支付超时。- 侧流
 *
 */

case class OrderInfo(orderId:String,orderAmount:Double,orderTime:Long,payState:String)

object PayCEPTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    val ds: KeyedStream[OrderInfo, String] = env.socketTextStream("nc_server", 9999)
      .map(line => {
        val split: Array[String] = line.split(",")
        OrderInfo(split(0), split(1).toDouble, split(2).toLong, split(3))
      }).assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[OrderInfo](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[OrderInfo] {
          override def extractTimestamp(t: OrderInfo, l: Long): Long = t.orderTime;
        }).withIdleness(Duration.ofSeconds(5))
    ).keyBy(_.orderId)

    val pattern: Pattern[OrderInfo, OrderInfo] = Pattern.begin[OrderInfo]("first")
      .where(_.payState.equals("create"))
      .followedBy("second").where(_.payState.equals("pay"))
      .within(Time.seconds(20))

    val patternStream: PatternStream[OrderInfo] = CEP.pattern(ds, pattern)

    //outputTag
    val outputTag = new OutputTag[String]("pay-timeout")

    val result: DataStream[String] = patternStream.process(new MyPatternProcessFunction(outputTag))
    result.print("支付成功")
    result.getSideOutput(outputTag).print("支付超时")
    env.execute()

  }

}

class MyPatternProcessFunction(outputTag:OutputTag[String]) extends PatternProcessFunction[OrderInfo,String]
with TimedOutPartialMatchHandler[OrderInfo]{
  //处理匹配数据
  override def processMatch(
                             map: util.Map[String, util.List[OrderInfo]],
                             context: PatternProcessFunction.Context,
                             collector: Collector[String]): Unit = {
    val orderID: String = map.get("first").get(0).orderId
    collector.collect(s"订单：$orderID 支付成功，待发货！")

  }
  //处理超时数据
  override def processTimedOutMatch(
                                     map: util.Map[String, util.List[OrderInfo]],
                                     context: PatternProcessFunction.Context): Unit = {
    val orderID: String = map.get("first").get(0).orderId
    context.output(outputTag,s"订单：$orderID 支付超时！")

  }
}