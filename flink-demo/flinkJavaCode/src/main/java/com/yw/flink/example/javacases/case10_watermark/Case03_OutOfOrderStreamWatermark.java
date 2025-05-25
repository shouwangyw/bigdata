package com.yw.flink.example.javacases.case10_watermark;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * Flink 乱序流中watermark设置
 * 案例:读取socket基站日志数据，每隔5s统计每个基站的通话时长
 */
public class Case03_OutOfOrderStreamWatermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取socket数据
        //001,181,182,busy,1000,10
        DataStreamSource<String> sourceDS = env.socketTextStream("node5", 9999);

        //转换数据流为StationLog 对象DataStream
        SingleOutputStreamOperator<StationLog> stationLogDS = sourceDS.map((MapFunction<String, StationLog>) s -> {
            String[] split = s.split(",");
            return new StationLog(
                    split[0],
                    split[1],
                    split[2],
                    split[3],
                    Long.valueOf(split[4]),
                    Long.valueOf(split[5]));
        });

        //设置watermark
        SingleOutputStreamOperator<StationLog> dsWithWatermark = stationLogDS.assignTimestampsAndWatermarks(
                //乱序流中设置watermark
                WatermarkStrategy.<StationLog>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        //从事件中选择事件时间列，需要是毫秒
                        .withTimestampAssigner((SerializableTimestampAssigner<StationLog>) (stationLog, l) -> stationLog.callTime)
                        //设置并行度空闲时间，自动推进watermark
                        .withIdleness(Duration.ofSeconds(5))
        );

        //设置窗口，每隔5s统计每个基站的通话时长
        dsWithWatermark.keyBy((KeySelector<StationLog, String>) stationLog -> stationLog.sid)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .sum("duration")
                .print();
        env.execute();
    }
}
