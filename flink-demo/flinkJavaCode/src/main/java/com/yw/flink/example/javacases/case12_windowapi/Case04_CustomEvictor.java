package com.yw.flink.example.javacases.case12_windowapi;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;

/**
 * Flink - window api - 实现自定义数据剔除器 Evictor
 * 案例:读取基站日志数据，设置globalwindow，手动指定触发器，每个基站每5秒生成窗口。
 */
public class Case04_CustomEvictor {
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

        //设置按照基站ID 分组,设置global窗口
        dsWithWatermark.keyBy(stationLog -> stationLog.sid)
                .window(GlobalWindows.create())
                .trigger(new MyTimeTrigger())
                //自定义数据剔除器，将迟到数据不计算到窗口计算
                .evictor(new Evictor<StationLog, GlobalWindow>() {
                    //窗口触发前执行调用，剔除数据
                    @Override
                    public void evictBefore(Iterable<TimestampedValue<StationLog>> iterable, int size, GlobalWindow globalWindow, EvictorContext evictorContext) {
                        //如果该窗口中数据callType为“迟到数据” 就剔除数据
                        Iterator<TimestampedValue<StationLog>> iter = iterable.iterator();
                        while (iter.hasNext()) {
                            TimestampedValue<StationLog> next = iter.next();
                            if ("迟到数据".equals(next.getValue().callType)) {
                                System.out.println("移除数据-" + next.getValue());
                                //删除当前指针对应的数据
                                iter.remove();
                            }
                        }
                    }
                    //窗口触发后执行调用，剔除数据，这种用的不多
                    @Override
                    public void evictAfter(Iterable<TimestampedValue<StationLog>> iterable, int i, GlobalWindow globalWindow, EvictorContext evictorContext) {
                    }
                }).process(new ProcessWindowFunction<StationLog, String, String, GlobalWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<StationLog, String, String, GlobalWindow>.Context context, Iterable<StationLog> elements, Collector<String> collector) throws Exception {
                        //统计该基站通话时长
                        long totalDurationTime = 0L;
                        for (StationLog element : elements) {
                            totalDurationTime += element.duration;
                        }

                        collector.collect("基站：" + key + ",通话总时长：" + totalDurationTime);

                    }
                }).print();
        env.execute();
    }

    private static class MyTimeTrigger extends Trigger<StationLog, GlobalWindow> {

        //设置状态，记录基站是否有未触发定时器
        private ValueStateDescriptor<Boolean> timerStateDescriptor = new ValueStateDescriptor<>("timer-state", Boolean.class);

        //来一条数据调用一次
        @Override
        public TriggerResult onElement(StationLog stationLog, long timestamp, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
            //获取当前基站ID的定时器状态
            Boolean isExist = triggerContext.getPartitionedState(timerStateDescriptor).value();
            if (isExist == null || !isExist) {
                //设置5秒后触发的定时器 + 4999
                triggerContext.registerEventTimeTimer(timestamp + 4999L);
                //更新状态
                triggerContext.getPartitionedState(timerStateDescriptor).update(true);
            }

            //如果此刻进入的事件时间，小于watermark，当前数据就是迟到数据，我们做标记，稍后剔除出窗口计算
            if (stationLog.callTime < triggerContext.getCurrentWatermark()) {
                stationLog.setCallType("迟到数据");
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
            //将状态更新为false
            triggerContext.getPartitionedState(timerStateDescriptor).update(false);
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public void clear(GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
            //...
        }
    }
}

