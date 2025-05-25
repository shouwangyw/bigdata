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
 * Flink 间断性生成watermark
 * 案例：读取socket基站日志数据，以基站001的事件时间为基准来触发窗口操作
 */
public class Case05_PunctuatedWatermarkGenerator {
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
                                new CustomPunctuatedWatermark())
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

    //以基站001为基准生成watermark
    private static class CustomPunctuatedWatermark implements WatermarkGenerator<StationLog> {
        //设置事件最大延迟时间
        long maxOutOfOrderness = 2000;

        long currentMaxTimestamp = Long.MIN_VALUE + maxOutOfOrderness + 1L;

        //每来一条数据调用一次
        @Override
        public void onEvent(StationLog stationLog, long eventTimeStamp, WatermarkOutput watermarkOutput) {
            //如果是基站001的数据，就生成watermark
            if ("001".equals(stationLog.getSid())) {
                //根据事件来获取 currentMaxTimestamp
                currentMaxTimestamp = Math.max(currentMaxTimestamp, stationLog.callTime);
                watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1L));
            }
        }

        //默认每200ms调用一次，时间周期可以通过 env.getConfig().setAutoWatermarkInterval() 来设置
        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            //这里不需要额外处理
        }
    }
}

