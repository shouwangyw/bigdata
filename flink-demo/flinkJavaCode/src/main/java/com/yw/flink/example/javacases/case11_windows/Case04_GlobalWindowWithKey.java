package com.yw.flink.example.javacases.case11_windows;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Flink - keyedStream全局窗口使用
 * 案例：读取socket基站日志数据，设置全局窗口，当每个基站有3条数据时，统计通话时长
 */
public class Case04_GlobalWindowWithKey {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取socket中数据
        //001,181,182,busy,1000,10
        DataStreamSource<String> sourceDs = env.socketTextStream("nc_server", 9999);

        //将String DataStream转换成StationLog类型的DataStream
        SingleOutputStreamOperator<StationLog> stationLogDS = sourceDs.map((MapFunction<String, StationLog>) s -> {
            String[] split = s.split(",");
            return new StationLog(split[0], split[1], split[2], split[3],
                    Long.valueOf(split[4]),
                    Long.valueOf(split[5]));
        });

        //设置watermark
        SingleOutputStreamOperator<StationLog> dsWithWatermark = stationLogDS.assignTimestampsAndWatermarks(
                //乱序流中设置watermark，指定最大的延迟时间为2秒
                WatermarkStrategy.<StationLog>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        //从事件中抽取事件时间列，这里需要是毫秒
                        .withTimestampAssigner((SerializableTimestampAssigner<StationLog>) (stationLog, l) -> stationLog.callTime)
                        //设置并行度空闲时间，自动推进watermark
                        .withIdleness(Duration.ofSeconds(5))
        );

        //进行分组
        KeyedStream<StationLog, String> keyedStream = dsWithWatermark.keyBy((KeySelector<StationLog, String>) stationLog -> stationLog.sid);

        //设置全局窗口，指定自定义触发逻辑
        keyedStream.window(GlobalWindows.create())
                //这里指定窗口触发逻辑，否则全局窗口不会触发
                .trigger(new MyCountTrigger())
                .process(new ProcessWindowFunction<StationLog, String, String, GlobalWindow>() {
                    @Override
                    public void process(String key,
                                        ProcessWindowFunction<StationLog, String, String, GlobalWindow>.Context context,
                                        Iterable<StationLog> elements,
                                        Collector<String> collector) throws Exception {
                        //统计通话时长
                        long totalDurationTime = 0L;
                        for (StationLog element : elements) {
                            totalDurationTime += element.duration;
                        }

                        collector.collect("基站ID:" + key + "，全局窗口触发，最近3条数据的通话时长：" + totalDurationTime);

                    }
                }).print();
        env.execute();

    }

    /**
     * 自定义trigger，当每个基站id来3条数据时，就触发窗口
     */
    private static class MyCountTrigger extends Trigger<StationLog, GlobalWindow> {
        //定义状态描述器
        private ValueStateDescriptor<Long> eventCountDescriptor = new ValueStateDescriptor<>("event-count", Long.class);

        //每来一条数据，调用一次
        @Override
        public TriggerResult onElement(StationLog stationLog, long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
            //获取状态值
            ValueState<Long> eventState = triggerContext.getPartitionedState(eventCountDescriptor);
            long count = eventState.value() == null ? 1L : eventState.value() + 1L;
            //设置状态值
            eventState.update(count);

            //判断状态值是否达到3条，达到3条触发，否则继续
            if (eventState.value() == 3L) {
                //该基站id对应的计数状态清空
                eventState.clear();
                return TriggerResult.FIRE_AND_PURGE;//触发窗口并清空窗口数据
            }

            return TriggerResult.CONTINUE;//什么都不做
        }

        //当自己定义的基于ProcessingTime 触发器触发时，会调用该方法
        @Override
        public TriggerResult onProcessingTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        //当自己定义的基于EventimeTime 触发器触发时，会调用该方法
        @Override
        public TriggerResult onEventTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        //当窗口销毁关闭时，会调用该方法，一般这个方法会清空状态
        @Override
        public void clear(GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
            //清空状态
            triggerContext.getPartitionedState(eventCountDescriptor).clear();

        }
    }
}

