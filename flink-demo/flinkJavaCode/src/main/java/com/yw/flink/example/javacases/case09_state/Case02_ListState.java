package com.yw.flink.example.javacases.case09_state;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Flink ListState状态测试
 * 案例：读取基站通话数据，每隔20s统计每个主叫号码通话总时长。
 * 001,186,187,busy,1000,10
 * 002,187,186,fail,2000,20
 * 003,186,188,busy,3000,30
 * 004,187,186,busy,4000,40
 * 005,189,187,busy,5000,50
 */
public class Case02_ListState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取socket数据
        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);
        //转换数据 ds -> StationLog
        SingleOutputStreamOperator<StationLog> stationLogDS = ds.map((MapFunction<String, StationLog>) line -> {
            String[] split = line.split(",");
            return new StationLog(split[0], split[1], split[2], split[3], Long.valueOf(split[4]), Long.valueOf(split[5]));
        });

        //对 stationLogDS 进行keyby ,key选择的是主叫
        KeyedStream<StationLog, String> keyedStream = stationLogDS.keyBy((KeySelector<StationLog, String>) stationLog -> stationLog.callOut);
        keyedStream.process(new KeyedProcessFunction<String, StationLog, String>() {
            private ListState<Long> listState;

            //状态注册与获取
            @Override
            public void open(Configuration parameters) throws Exception {
                //创建状态描述器，注册状态
                ListStateDescriptor<Long> listStateDescriptor = new ListStateDescriptor<>("listState", Long.class);
                //获取状态
                listState = getRuntimeContext().getListState(listStateDescriptor);

            }

            //每有一条数据调用一次
            @Override
            public void processElement(StationLog stationLog, KeyedProcessFunction<String, StationLog, String>.Context context, Collector<String> collector) throws Exception {

                //获取每个key 主叫对应的列表状态
                Iterable<Long> iterable = listState.get();
                //如果状态中没有值，说明对该主叫注册定时器，20s后触发
                if (!iterable.iterator().hasNext()) {
                    //获取当前处理数据的时间
                    long time = context.timerService().currentProcessingTime();
                    //注册定时器
                    context.timerService().registerProcessingTimeTimer(time + 20 * 1000L);

                }
                //向列表状态中追加数据
                listState.add(stationLog.duration);
            }

            //定时器触发调用方法
            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, StationLog, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                //获取当期key 主叫 对应的 listState值，累计获取总的通话时长
                Iterable<Long> iterable = listState.get();
                //定义通话总时长
                Long totalCallTime = 0L;

                for (Long duration : iterable) {
                    totalCallTime += duration;
                }
                out.collect("当前主叫号码：" + ctx.getCurrentKey() + ",最近20s通话总时长 ：" + totalCallTime);

                //清空状态
                listState.clear();
            }
        }).print();
        env.execute();
    }
}
