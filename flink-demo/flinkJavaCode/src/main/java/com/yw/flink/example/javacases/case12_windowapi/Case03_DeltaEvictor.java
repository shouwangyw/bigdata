package com.yw.flink.example.javacases.case12_windowapi;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.DeltaEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Flink - window api - evictor 使用
 * 案例：读取socket中基站日志数据，每隔5秒统计每个基站的通话时长
 * 要求：如果一个窗口中的数据与该窗口最后一条数据通话时长相差5秒，就剔除数据
 */
public class Case03_DeltaEvictor {
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

        //按照基站分组，并设置滚动窗口
        dsWithWatermark.keyBy(stationLog -> stationLog.sid)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //使用内置的DeltaEvictor，实现数据剔除
                .evictor(DeltaEvictor.of(5, (DeltaFunction<StationLog>) (oldDataPoint, newDataPoint) -> Math.abs(newDataPoint.duration - oldDataPoint.duration)))
                .process(new ProcessWindowFunction<StationLog, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<StationLog, String, String, TimeWindow>.Context context, Iterable<StationLog> elements, Collector<String> collector) throws Exception {
                        //获取窗口的起始时间
                        long startTime = context.window().getStart();
                        long endTime = context.window().getEnd();

                        //统计基站通话总时长
                        long totalDurationTime = 0L;
                        for (StationLog element : elements) {
                            totalDurationTime += element.duration;
                        }
                        collector.collect("当前窗口范围：[" + startTime + "~" + endTime + "),基站ID:" + key + ",通话总时长：" + totalDurationTime);
                    }
                }).print();
        env.execute();
    }
}
