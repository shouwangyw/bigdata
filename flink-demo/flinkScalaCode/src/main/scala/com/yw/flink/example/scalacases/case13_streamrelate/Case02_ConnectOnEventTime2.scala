package com.yw.flink.example.scalacases.case13_streamrelate

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import java.time.Duration

/**
  * 案例：读取订单流和支付流，超过一定时间后进行报警
  * 1.订单流来一条数据，超过5秒如果没有支付信息就报警：有订单信息，没有支付信息
  * 2.支付流来一条数据，超过5秒如果没有订单信息就报警：有支付信息，没有订单信息
  */
object Case02_ConnectOnEventTime2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //读取socket 订单流：订单ID，用户ID，订单金额，时间戳
    //order1,user1,10,1000
    val orderDS: DataStream[String] = env.socketTextStream("node5", 8888)
    //设置订单流的watermark
    val orderDSWithWatermark: DataStream[String] = orderDS.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[String](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[String] {
          override def extractTimestamp(t: String, l: Long): Long = t.split(",")(3).toLong
        })
    )

    //读取socket 支付流：订单ID，支付金额，支付时间戳
    //order1,10,1000
    val payDS: DataStream[String] = env.socketTextStream("node5", 9999)
    //设置支付流的watermark
    val payDSWithWatermark: DataStream[String] = payDS.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[String](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[String] {
          override def extractTimestamp(t: String, l: Long): Long = t.split(",")(2).toLong
        })
    )

    //两流进行关联
    orderDSWithWatermark.keyBy(_.split(",")(0))
      .connect(payDSWithWatermark.keyBy(_.split(",")(0)))
      .process(new KeyedCoProcessFunction[String, String, String, String] {
        //设置状态
        var orderState: ValueState[String] = _
        var payState: ValueState[String] = _

        //初始化状态
        override def open(parameters: Configuration): Unit = {
          //设置状态描述器
          val orderStateDescriptor = new ValueStateDescriptor[String]("order-state", classOf[String])
          val payStateDescriptor = new ValueStateDescriptor[String]("pay-state", classOf[String])

          orderState = getRuntimeContext.getState(orderStateDescriptor)
          payState = getRuntimeContext.getState(payStateDescriptor)

        }

        //处理订单流
        override def processElement1(orderInfo: String,
                                     context: KeyedCoProcessFunction[String, String, String, String]#Context,
                                     collector: Collector[String]): Unit = {
          //来一条订单信息，判断支付状态是否存在该订单的支付信息
          if (payState.value() == null) {
            //获取订单事件时间
            val orderTimestamp: Long = orderInfo.split(",")(3).toLong
            //注册定时器
            context.timerService().registerEventTimeTimer(orderTimestamp + 5 * 1000L)

            //更新状态
            orderState.update(orderInfo)

          } else {
            //如果支付状态不为空，说明订单已经支付，删除定时器
            context.timerService().deleteEventTimeTimer(payState.value().split(",")(2).toLong + 5 * 1000L)
            //清空状态
            payState.clear()
          }
        }

        //处理支付流
        override def processElement2(payInfo: String,
                                     context: KeyedCoProcessFunction[String, String, String, String]#Context,
                                     collector: Collector[String]): Unit = {
          //当来一条支付数据后，判断订单状态是否为空，如果为空，说明订单有支付，没有订单信息，注册定时器
          if (orderState.value() == null) {
            //注册定时器
            context.timerService().registerEventTimeTimer(payInfo.split(",")(2).toLong + 5 * 1000L)
            //更新支付状态
            payState.update(payInfo)

          } else {
            //说明订单信息和支付信息都有了，那就删除定时器
            context.timerService().deleteEventTimeTimer(orderState.value().split(",")(3).toLong + 5 * 1000L)
            //清空订单状态
            orderState.clear()
          }
        }

        //定时器触发之后执行
        override def onTimer(timestamp: Long,
                             ctx: KeyedCoProcessFunction[String, String, String, String]#OnTimerContext,
                             out: Collector[String]): Unit = {
          //如果订单状态中有值，说明该订单有订单信息，没有支付信息
          if (orderState.value() != null) {
            //输出报警
            out.collect(s"订单ID:${orderState.value().split(",")(0)} ,已经超过5秒没有支付，请尽快支付！")
            //清空状态
            orderState.clear()

          }

          //如果支付状态中有值，说明该订单有支付信息，没有订单详情
          if (payState.value() != null) {
            //输出报警信息
            out.collect(s"订单ID:${payState.value().split(",")(0)} 有异常，有支付信息，没有订单信息！")

            //清空状态
            payState.clear()
          }

        }
      }).print()
    env.execute()
  }
}
