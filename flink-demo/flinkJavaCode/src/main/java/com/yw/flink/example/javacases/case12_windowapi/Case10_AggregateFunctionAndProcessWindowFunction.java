package com.yw.flink.example.javacases.case12_windowapi;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Flink - 增量和全量窗口聚合函数结合使用
 * 案例：读取socket基站日志数据，每隔5s统计每个基站平均通话时长。
 */
public class Case10_AggregateFunctionAndProcessWindowFunction {
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

        dsWithWatermark.keyBy(stationLog -> stationLog.sid)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<StationLog, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
                    //创建累加器
                    @Override
                    public Tuple2<Long, Long> createAccumulator() {
                        //第一位：当前sid的通话时长；第二位：当前sid的通话次数
                        return Tuple2.of(0L, 0L);
                    }

                    //每来一条数据做一次处理
                    @Override
                    public Tuple2<Long, Long> add(StationLog stationLog, Tuple2<Long, Long> acc) {
                        return Tuple2.of(stationLog.duration + acc.f0, acc.f1 + 1L);
                    }

                    //返回结果
                    @Override
                    public Tuple2<Long, Long> getResult(Tuple2<Long, Long> acc) {
                        return acc;
                    }

                    //累加器合并
                    @Override
                    public Tuple2<Long, Long> merge(Tuple2<Long, Long> longLongTuple2, Tuple2<Long, Long> acc1) {
                        return null;
                    }
                }, new ProcessWindowFunction<Tuple2<Long, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        ProcessWindowFunction<Tuple2<Long, Long>, String, String, TimeWindow>.Context context,
                                        Iterable<Tuple2<Long, Long>> iterable, Collector<String> collector) throws Exception {
                        //获取窗口的起始时间
                        long startTime = context.window().getStart();
                        long endTime = context.window().getEnd();

                        //获取平均通话时长
                        Tuple2<Long, Long> next = iterable.iterator().next();
                        double avgDuration = (double) (next.f0 / next.f1);
                        collector.collect("窗口范围：[" + startTime + "~" + endTime + "),基站ID:" + key + ",平均通话时长：" + avgDuration);

                    }
                }).print();
        env.execute();
    }
}
