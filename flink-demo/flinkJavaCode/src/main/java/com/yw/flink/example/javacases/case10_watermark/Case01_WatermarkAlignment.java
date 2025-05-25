package com.yw.flink.example.javacases.case10_watermark;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * Flink Watermark 对齐机制代码实现
 * 案例：读取socket基站数据，每个5s计算每个基站的通话总时长
 */
public class Case01_WatermarkAlignment {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取socket数据
        //001,181,182,busy,1000,10
        DataStreamSource<String> sourceDS = env.socketTextStream("node5", 9999);

        //转换数据流为StationLog 对象DataStream
        SingleOutputStreamOperator<StationLog> stationLogDS = sourceDS.map((MapFunction<String, StationLog>) s -> {
            String[] split = s.split(",");
            return new StationLog(split[0], split[1], split[2], split[3], Long.valueOf(split[4]), Long.valueOf(split[5]));
        });

        //设置watermark以及watermark对齐
        SingleOutputStreamOperator<StationLog> dsWithWatermark = stationLogDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<StationLog>forMonotonousTimestamps()
                        //选择事件时间列
                        .withTimestampAssigner((SerializableTimestampAssigner<StationLog>) (stationLog, l) -> stationLog.callTime)
                        .withIdleness(Duration.ofSeconds(5))
                        //设置watermark 对齐机制，第一个参数：设置多个并行度/多个源 为同一个watermark对齐组，
                        //第二个参数：设置watermark 最大偏移值
                        //第三个参数：设置检查周期
                        .withWatermarkAlignment("socket-source-group", Duration.ofSeconds(5), Duration.ofSeconds(2))
        );

        //设置窗口
        dsWithWatermark.keyBy(stationLog -> stationLog.sid)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("duration").print();

        env.execute();
    }
}
