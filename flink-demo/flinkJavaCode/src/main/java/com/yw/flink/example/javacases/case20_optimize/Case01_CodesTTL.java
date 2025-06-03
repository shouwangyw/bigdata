package com.yw.flink.example.javacases.case20_optimize;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 统计每个基站的通话时长
 * 大状态中设置TTL
 */
public class Case01_CodesTTL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = StreamExecutionEnvironment.getExecutionEnvironment()
                .socketTextStream("nc_server", 9999);
        //001,186,187,busy,1000,10
        SingleOutputStreamOperator<StationLog> stationLogDS = ds.map((MapFunction<String, StationLog>) s -> {
            String[] arr = s.split(",");
            return new StationLog(arr[0], arr[1], arr[2], arr[3], Long.valueOf(arr[4]), Long.valueOf(arr[5]));
        });

        stationLogDS.keyBy(stationLog -> stationLog.sid)
                .map(new RichMapFunction<StationLog, String>() {
                    private ValueState<Long> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //定义状态TTL
                        StateTtlConfig ttlConfig = StateTtlConfig
                                .newBuilder(Time.seconds(5))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();

                        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("value-state", Long.class);

                        descriptor.enableTimeToLive(ttlConfig);

                        valueState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public String map(StationLog stationLog) throws Exception {
                        //判断状态值
                        Long stateValue = valueState.value();
                        if (stateValue == null) {
                            //说明当前基站没有状态
                            valueState.update(stationLog.duration);
                        } else {
                            //说明当前基站有状态，更新
                            valueState.update(stateValue + stationLog.duration);
                        }
                        return "基站：" + stationLog.sid + ",通话时长：" + valueState.value();
                    }
                }).print();
        env.execute();
    }
}
