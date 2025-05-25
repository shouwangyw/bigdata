package com.yw.flink.example.javacases.case11_windows;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Flink - 针对DataStream设置滑动窗口
 * 案例：读取socket基站日志数据，每隔5秒统计最近10秒所有基站通话时长
 */
public class Case02_SlidingWindowWithoutKey {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取socket中数据
        //001,181,182,busy,1000,10
        DataStreamSource<String> sourceDs = env.socketTextStream("node5", 9999);

        //将String DataStream转换成StationLog类型的DataStream
        SingleOutputStreamOperator<StationLog> stationLogDS = sourceDs.map(new MapFunction<String, StationLog>() {
            @Override
            public StationLog map(String s) throws Exception {
                String[] split = s.split(",");
                return new StationLog(split[0], split[1], split[2], split[3],
                        Long.valueOf(split[4]),
                        Long.valueOf(split[5]));
            }
        });

        //设置watermark
        SingleOutputStreamOperator<StationLog> dsWithWatermark = stationLogDS.assignTimestampsAndWatermarks(
                //乱序流中设置watermark，指定最大的延迟时间为2秒
                WatermarkStrategy.<StationLog>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        //从事件中抽取事件时间列，这里需要是毫秒
                        .withTimestampAssigner(new SerializableTimestampAssigner<StationLog>() {
                            @Override
                            public long extractTimestamp(StationLog stationLog, long l) {
                                return stationLog.callTime;
                            }
                        })
                        //设置并行度空闲时间，自动推进watermark
                        .withIdleness(Duration.ofSeconds(5))
        );


        //设置滑动窗口
        dsWithWatermark.windowAll(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .process(new ProcessAllWindowFunction<StationLog, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<StationLog, String, TimeWindow>.Context context,
                                        Iterable<StationLog> elements,
                                        Collector<String> collector) throws Exception {
                        //获取窗口起始时间
                        long startTime = context.window().getStart() <0 ? 0 : context.window().getStart();
                        long endTime = context.window().getEnd();
                        //获取当期基站通话时长
                        long totalDurationTime = 0L;
                        for (StationLog element : elements) {
                            totalDurationTime += element.getDuration();
                        }
                        //输出数据
                        collector.collect("窗口范围：["+startTime+"~"+endTime+"),所有基站通话时长:"+totalDurationTime);

                    }
                }).print();


        env.execute();
    }
}
