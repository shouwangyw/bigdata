package com.yw.flink.example.javacases.case13_streamrelate;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Flink - Interval Join
 * 案例：读取用户登录流和广告点击流，通过Interval Join分析用户登录前后广告点击的行为
 */
public class Case04_IntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取用户登录流，用户ID,登录时间
        //user1,1000
        //对登录流设置watermark
        SingleOutputStreamOperator<String> loginDsWithWatermark = env.socketTextStream("node5", 8888)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((SerializableTimestampAssigner<String>) (s, l) -> Long.parseLong(s.split(",")[1]))
                );

        //读取广告点击流，
        // 用户ID,广告ID,点击时间
        //user1,product1,1000
        //对登录流设置watermark
        SingleOutputStreamOperator<String> clickDsWithWatermark = env.socketTextStream("node5", 9999)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((SerializableTimestampAssigner<String>) (s, l) -> Long.parseLong(s.split(",")[2]))
                );

        //对两条流进行interval join
        loginDsWithWatermark.keyBy((KeySelector<String, String>) s -> s.split(",")[0])
                .intervalJoin(clickDsWithWatermark.keyBy((KeySelector<String, String>) s -> s.split(",")[0]))
                //设置时间范围
                .between(Time.seconds(-2), Time.seconds(2))
                .lowerBoundExclusive()
                .upperBoundExclusive()
                .process(new ProcessJoinFunction<String, String, String>() {
                    @Override
                    public void processElement(String left,
                                               String right,
                                               ProcessJoinFunction<String, String, String>.Context context,
                                               Collector<String> collector) throws Exception {
                        //获取用户
                        String userId = left.split(",")[0];
                        collector.collect("用户：" + userId + ",点击了广告：" + right);
                    }
                }).print();
        env.execute();
    }
}
