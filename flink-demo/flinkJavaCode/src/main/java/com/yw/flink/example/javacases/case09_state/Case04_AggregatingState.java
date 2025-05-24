package com.yw.flink.example.javacases.case09_state;

import com.yw.flink.example.StationLog;
import lombok.val;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink AggregatingState 聚合状态测试
 * 案例：读取基站日志数据，统计每个主叫号码通话平均时长。
 * 001,186,187,busy,1000,10
 * 002,187,186,fail,2000,20
 * 003,186,188,busy,3000,30
 * 004,187,186,busy,4000,40
 * 005,186,187,busy,5000,50
 */
public class Case04_AggregatingState {
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
        KeyedStream<StationLog, String> keyedStream = stationLogDS.keyBy(
                (KeySelector<StationLog, String>) stationLog -> stationLog.callOut);

        //统计每个主叫号码的平均通话时长
        keyedStream.map(new RichMapFunction<StationLog, String>() {
            private AggregatingState<StationLog, Double> aggregatingState;

            //注册状态并获取状态
            @Override
            public void open(Configuration parameters) throws Exception {
                //注册AggregatingState
                val aggregatingStateDescriptor = new AggregatingStateDescriptor<>("aggregatingState",
                        new AggregateFunction<StationLog, Tuple2<Long, Long>, Double>() {

                            //创建累加器，通过这个tuple存储当前主叫的通话次数以及通话总时长
                            @Override
                            public Tuple2<Long, Long> createAccumulator() {
                                return new Tuple2<>(0L, 0L);
                            }

                            //每向AggregatingState中添加一条数据，就会调用这个add方法
                            @Override
                            public Tuple2<Long, Long> add(StationLog stationLog, Tuple2<Long, Long> acc) {
                                return new Tuple2<>(acc.f0 + 1, acc.f1 + stationLog.duration);
                            }

                            //返回累加器结果
                            @Override
                            public Double getResult(Tuple2<Long, Long> acc) {
                                return Double.valueOf(acc.f1) / Double.valueOf(acc.f0);
                            }

                            //合并两个累加器的值
                            @Override
                            public Tuple2<Long, Long> merge(Tuple2<Long, Long> acc1, Tuple2<Long, Long> acc2) {
                                return new Tuple2<>(acc1.f0 + acc2.f0, acc1.f1 + acc2.f1);
                            }
                        }, Types.TUPLE(Types.LONG, Types.LONG));

                //获取状态
                aggregatingState = getRuntimeContext().getAggregatingState(aggregatingStateDescriptor);
            }

            @Override
            public String map(StationLog stationLog) throws Exception {
                //向状态中添加元素
                aggregatingState.add(stationLog);
                return "当前主叫：" + stationLog.callOut + ",截止到目前为止平均通话时长为：" + aggregatingState.get();
            }
        }).print();
        env.execute();
    }
}
