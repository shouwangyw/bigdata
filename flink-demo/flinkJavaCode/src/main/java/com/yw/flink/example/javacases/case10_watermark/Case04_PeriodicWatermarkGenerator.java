package com.yw.flink.example.javacases.case10_watermark;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * Flink 自定义watermark生成，设置事件最大延迟时间时2000ms
 * 实现的就是forBoundedOutOfOrderness
 */
public class Case04_PeriodicWatermarkGenerator {
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


        //设置自定义watermark
        SingleOutputStreamOperator<StationLog> dsWithWatermark = stationLogDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.forGenerator((WatermarkGeneratorSupplier<StationLog>) context ->
                                new CustomPeriodicWatermark())
                        .withTimestampAssigner((SerializableTimestampAssigner<StationLog>) (stationLog, l) -> stationLog.callTime)
                        .withIdleness(Duration.ofSeconds(5))
        );

        //设置窗口
        dsWithWatermark.keyBy(stationLog -> stationLog.sid)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("duration")
                .print();
        env.execute();
    }

    private static class CustomPeriodicWatermark implements WatermarkGenerator<StationLog> {
        //设置事件最大延迟时间
        long maxOutOfOrderness = 2000;

        //设置当前事件的最大时间戳
        long currentMaxTimestamp = Long.MIN_VALUE + maxOutOfOrderness + 1L;

        //每来一条数据调用一次
        @Override
        public void onEvent(StationLog stationLog, long eventTimeStamp, WatermarkOutput watermarkOutput) {
            //根据事件来获取 currentMaxTimestamp
            currentMaxTimestamp = Math.max(currentMaxTimestamp, stationLog.callTime);
        }


        //默认每200ms调用一次，时间周期可以通过 env.getConfig().setAutoWatermarkInterval() 来设置
        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1L));

        }
    }
}


