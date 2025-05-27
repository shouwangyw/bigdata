package com.yw.flink.example.javacases.case12_windowapi;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * Flink : Flink Window 窗口函数 - AggregateFunction
 * 案例：读取socket基站日志数据，每隔5s统计每个基站通话总时长
 */
public class Case06_AggregateFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取socket中数据
        //001,181,182,busy,1000,10
        DataStreamSource<String> sourceDs = env.socketTextStream("node5", 9999);

        //将String DataStream转换成StationLog类型的DataStream
        SingleOutputStreamOperator<StationLog> stationLogDS = sourceDs.map((MapFunction<String, StationLog>) s -> {
            String[] split = s.split(",");
            return new StationLog(split[0], split[1], split[2], split[3], Long.valueOf(split[4]), Long.valueOf(split[5]));
        });

        //设置watermark
        SingleOutputStreamOperator<StationLog> dsWithWatermark = stationLogDS.assignTimestampsAndWatermarks(
                //乱序流中设置watermark，指定最大的延迟时间为2秒
                WatermarkStrategy.<StationLog>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        //从事件中抽取事件时间列，这里需要是毫秒
                        .withTimestampAssigner((SerializableTimestampAssigner<StationLog>) (stationLog, l) -> stationLog.callTime)
                        //设置并行度空闲时间，自动推进watermark
                        .withIdleness(Duration.ofSeconds(5)));
        //对数据进行转换
        KeyedStream<Tuple2<String, Long>, String> keyStream = dsWithWatermark
                .map((MapFunction<StationLog, Tuple2<String, Long>>) stationLog -> Tuple2.of(stationLog.sid, stationLog.duration)).keyBy(tp -> tp.f0);

        SingleOutputStreamOperator<String> result = keyStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>() {
                    //创建累加器，这个就是贯穿整个增量计算的状态
                    @Override
                    public Tuple2<String, Long> createAccumulator() {
                        return Tuple2.of("", 0L);
                    }

                    //有一条数据调用一次
                    @Override
                    public Tuple2<String, Long> add(Tuple2<String, Long> value, Tuple2<String, Long> accumulator) {
                        return Tuple2.of(value.f0, value.f1 + accumulator.f1);
                    }

                    //返回结果
                    @Override
                    public String getResult(Tuple2<String, Long> accumulator) {
                        return "基站ID:" + accumulator.f0 + ",通话时长：" + accumulator.f1;
                    }

                    //合并窗口调用，合并就是每个窗口的累加器
                    @Override
                    public Tuple2<String, Long> merge(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> acc1) {
                        return null;
                    }
                });

        result.print();
        env.execute();
    }
}
