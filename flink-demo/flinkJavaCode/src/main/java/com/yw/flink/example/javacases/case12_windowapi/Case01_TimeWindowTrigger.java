package com.yw.flink.example.javacases.case12_windowapi;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Flink windowapi - 时间窗口的自定义触发器
 * 案例：针对时间窗口设置自定义触发器，每有一条数据触发一次窗口执行
 */
public class Case01_TimeWindowTrigger {
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

        //针对基站id进行分组，然后设置每5s的滚动窗口
        KeyedStream<StationLog, String> keyedStream = dsWithWatermark.keyBy((KeySelector<StationLog, String>) stationLog -> stationLog.sid);

        keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //自定义窗口触发，每条数据都触发窗口执行
                .trigger(new MyTimeTrigger())
                .process(new ProcessWindowFunction<StationLog, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<StationLog, String, String, TimeWindow>.Context context, Iterable<StationLog> elements, Collector<String> collector) throws Exception {
                        //统计通话时长
                        long totalDurationTime = 0L;
                        for (StationLog element : elements) {
                            totalDurationTime += element.duration;
                        }

                        //设置窗口的起始时间
                        long startTime = context.window().getStart();
                        long endTime = context.window().getEnd();
                        collector.collect("窗口范围：[" + startTime + "~" + endTime + "),基站ID:" + key + ",通话时长：" + totalDurationTime);

                    }
                }).print();
        env.execute();
    }

    private static class MyTimeTrigger extends Trigger<StationLog, TimeWindow> {

        //每来一条数据执行一次
        @Override
        public TriggerResult onElement(StationLog stationLog, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            System.out.println("==== onElement方法执行 ====");
            return TriggerResult.FIRE_AND_PURGE;
        }

        //当基于ProcessTime的触发器触发时调用
        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            System.out.println("==== onProcessingTime 方法执行 ====");

            return TriggerResult.CONTINUE;
        }

        //当基于EventTime的触发器触发时调用
        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            System.out.println("==== onEventTime 方法执行 ====");

            return TriggerResult.CONTINUE;
        }

        //当窗口销毁时调用，只是针对时间窗口销毁才会触发
        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            System.out.println("==== clear 方法执行 ====");

        }
    }
}


