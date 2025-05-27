package com.yw.flink.example.javacases.case13_streamrelate;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * Flink - Window cogroup
 * 案例：读取订单流和支付流，进行window cogroup
 */
public class Case05_WindowCogroup {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度为1
        env.setParallelism(1);

        //读取socket 订单流：订单ID，用户ID，订单金额，时间戳
        //order1,user1,10,1000
        //对订单流设置watermark
        SingleOutputStreamOperator<String> orderDSWithWatermark = env.socketTextStream("node5", 8888)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((SerializableTimestampAssigner<String>) (s, l) -> Long.parseLong(s.split(",")[3]))
                );

        //读取socket 支付流：订单ID，支付金额，支付时间戳
        //order1,10,1000
        //对支付流设置watermark
        SingleOutputStreamOperator<String> payDSWithWatermark = env.socketTextStream("node5", 9999)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((SerializableTimestampAssigner<String>) (s, l) -> Long.parseLong(s.split(",")[2]))
                );

        //对两个流进行window cogroup
        orderDSWithWatermark.coGroup(payDSWithWatermark)
                .where((KeySelector<String, String>) s -> s.split(",")[0])
                .equalTo((KeySelector<String, String>) s -> s.split(",")[0])
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply((CoGroupFunction<String, String, String>) (left, right, collector) -> collector.collect(left + " ==== " + right)).print();

        env.execute();
    }
}
