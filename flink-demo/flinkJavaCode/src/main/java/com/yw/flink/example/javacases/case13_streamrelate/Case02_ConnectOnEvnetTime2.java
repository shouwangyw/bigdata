package com.yw.flink.example.javacases.case13_streamrelate;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 案例：读取订单流和支付流，超过一定时间后进行报警
 * 1.订单流来一条数据，超过5秒如果没有支付信息就报警：有订单信息，没有支付信息
 * 2.支付流来一条数据，超过5秒如果没有订单信息就报警：有支付信息，没有订单信息
 */
public class Case02_ConnectOnEvnetTime2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度为1
        env.setParallelism(1);

        //读取socket 订单流：订单ID，用户ID，订单金额，时间戳
        //order1,user1,10,1000
        //对订单流设置watermark
        SingleOutputStreamOperator<String> orderDsWithWatermark = env
                .socketTextStream("node5", 8888)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((SerializableTimestampAssigner<String>) (s, l) -> Long.parseLong(s.split(",")[3]))
                );

        //读取socket 支付流：订单ID，支付金额，支付时间戳
        //order1,10,1000
        //对支付流设置watermark
        SingleOutputStreamOperator<String> payDsWithWatermark = env
                .socketTextStream("node5", 9999)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((SerializableTimestampAssigner<String>) (s, l) -> Long.parseLong(s.split(",")[2]))
                );

        //对两流进行关联

        orderDsWithWatermark
                .keyBy((KeySelector<String, String>) s -> s.split(",")[0]) //订单id
                .connect(payDsWithWatermark.keyBy((KeySelector<String, String>) s -> s.split(",")[0])) //订单id
                .process(new KeyedCoProcessFunction<String, String, String, String>() {
                    //设置订单状态：方便来了支付信息后，查看订单是否存在
                    private ValueState<String> orderState = null;

                    //设置支付状态：方便来了订单信息后，查看支付是否存在
                    private ValueState<String> payState = null;

                    //初始化状态
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //设置状态描述器
                        ValueStateDescriptor<String> orderStateDescriptor = new ValueStateDescriptor<>("order-state", String.class);
                        ValueStateDescriptor<String> payStateDescriptor = new ValueStateDescriptor<>("pay-state", String.class);

                        //初始化状态
                        orderState = getRuntimeContext().getState(orderStateDescriptor);
                        payState = getRuntimeContext().getState(payStateDescriptor);
                    }

                    //处理订单流
                    @Override
                    public void processElement1(String orderInfo,
                                                KeyedCoProcessFunction<String, String, String, String>.Context context,
                                                Collector<String> collector) throws Exception {
                        //来一条订单信息后，判断支付状态中是否有该订单的支付信息，
                        if (payState.value() == null) {
                            //如果没有该订单支付信息，设置5秒后触发的定时器
                            //获取订单事件的时间
                            long orderTimestamp = Long.parseLong(orderInfo.split(",")[3]);
                            //注册定时器
                            context.timerService().registerEventTimeTimer(orderTimestamp + 5 * 1000L);

                            //将订单信息存入状态
                            orderState.update(orderInfo);
                        } else {
                            //如果支付状态中有该订单的支付信息，将定时器删除
                            long triggerTime = Long.parseLong(payState.value().split(",")[2]) + 5 * 1000L;
                            //删除定时器
                            context.timerService().deleteEventTimeTimer(triggerTime);
                            //清空状态，清空支付状态
                            payState.clear();
                        }

                    }


                    //处理支付流
                    @Override
                    public void processElement2(String payInfo,
                                                KeyedCoProcessFunction<String, String, String, String>.Context context,
                                                Collector<String> collector) throws Exception {
                        //当来一条订单支付信息时，判断订单状态中是否有该订单信息
                        if (orderState.value() == null) {
                            //如果没有注册5秒后触发的定时器
                            //获取支付信息的事件时间
                            long payTimestamp = Long.parseLong(payInfo.split(",")[2]);
                            //注册定时器
                            context.timerService().registerEventTimeTimer(payTimestamp + 5 * 1000L);
                            //把当前支付信息更新到状态中
                            payState.update(payInfo);

                        } else {
                            //如果订单状态不为空，说明该订单有订单信息，也有了支付信息，就需要删除定时器
                            //获取定时器触发时间
                            long triggerTime = Long.parseLong(orderState.value().split(",")[3]) + 5 * 1000L;
                            //删除定时器
                            context.timerService().deleteEventTimeTimer(triggerTime);
                            //清空状态
                            orderState.clear();
                        }

                    }

                    //定时器触发调用的方法
                    @Override
                    public void onTimer(long timestamp,
                                        KeyedCoProcessFunction<String, String, String, String>.OnTimerContext ctx,
                                        Collector<String> out) throws Exception {
                        //如果订单状态中有值，那么代表有订单信息，没有支付信息
                        if (orderState.value() != null) {
                            //输出信息
                            out.collect("订单ID:" + orderState.value().split(",")[0] + ",该订单已经超过5秒没有支付，请尽快支付！");
                            //清空状态
                            orderState.clear();
                        }

                        //如果支付状态中有值，那么代表有订单支付信息，没有订单详细信息
                        if (payState.value() != null) {
                            //输出信息
                            out.collect("订单ID:" + payState.value().split(",")[0] + ",该订单异常，有支付信息，但是5秒内没有获取订单信息！");
                            //清空状态
                            payState.clear();
                        }
                    }

                }).print();
        env.execute();
    }
}
