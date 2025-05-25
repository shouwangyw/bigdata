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
 * Flink 有序流中设置watermark
 * 案例:读取socket基站日志数据，每隔5s统计每个基站的通话时长
 */
public class Case02_InOrderStreamEventWatermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        //读取socket数据
        //001,181,182,busy,1000,10
        DataStreamSource<String> sourceDS = env.socketTextStream("node5", 9999);

        //转换数据流为StationLog 对象DataStream
        SingleOutputStreamOperator<StationLog> stationLogDS = sourceDS.map((MapFunction<String, StationLog>) s -> {
            String[] split = s.split(",");
            return new StationLog(split[0], split[1], split[2], split[3], Long.valueOf(split[4]), Long.valueOf(split[5]));
        });

        //设置watermark
        SingleOutputStreamOperator<StationLog> dsWithWatermark = stationLogDS.assignTimestampsAndWatermarks(
                //给有序事件流设置watermark
                WatermarkStrategy.<StationLog>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<StationLog>) (stationLog, timestamp) -> {
                            //从数据流中选择事件时间列，这里选择的列必须是毫秒
                            return stationLog.callTime;
                        })
                        //当某些并行度超过该空闲时间后，自动推进watermark
                        .withIdleness(Duration.ofSeconds(5))

        );

        //设置窗口，每隔5秒统计每个基站的通话总时长
        dsWithWatermark.keyBy(stationLog -> stationLog.sid)
                //设置滚动窗口，每隔5生成一个
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("duration").print();
        env.execute();
    }
}
