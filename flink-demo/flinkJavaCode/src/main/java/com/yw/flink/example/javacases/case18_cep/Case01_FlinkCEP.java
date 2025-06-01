package com.yw.flink.example.javacases.case18_cep;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Flink CEP Test
 * 案例：读取基站日志通话数据，当一个基站通话状态连续3次失败，就报警。
 */
public class Case01_FlinkCEP {
    public static void main(String[] args) throws Exception {
        //1.准备事件流
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //001,181,182,busy,1000,10
        SingleOutputStreamOperator<StationLog> ds = env.socketTextStream("nc_server", 9999)
                .map((MapFunction<String, StationLog>) s -> {
                    String[] split = s.split(",");
                    return new StationLog(
                            split[0],
                            split[1],
                            split[2],
                            split[3],
                            Long.valueOf(split[4]),
                            Long.valueOf(split[5])
                    );
                });

        SingleOutputStreamOperator<StationLog> dsWithWatermark = ds.assignTimestampsAndWatermarks(
                WatermarkStrategy.<StationLog>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((SerializableTimestampAssigner<StationLog>) (stationLog, l) -> stationLog.callTime)
                        .withIdleness(Duration.ofSeconds(5))
        );


        KeyedStream<StationLog, String> keyedStream = dsWithWatermark.keyBy((KeySelector<StationLog, String>) stationLog -> stationLog.sid);


        //2.定义匹配的规则-准备pattern
        Pattern<StationLog, StationLog> pattern = Pattern.<StationLog>begin("first").where(new SimpleCondition<StationLog>() {
                    @Override
                    public boolean filter(StationLog stationLog) throws Exception {
                        return stationLog.callType.equals("fail");
                    }
                })
                .next("second")
                .where(new SimpleCondition<StationLog>() {
                    @Override
                    public boolean filter(StationLog stationLog) throws Exception {
                        return stationLog.callType.equals("fail");
                    }
                })
                .next("third").where(new SimpleCondition<StationLog>() {
                    @Override
                    public boolean filter(StationLog stationLog) throws Exception {
                        return stationLog.callType.equals("fail");
                    }
                });


        //3.模式应用到数据流中
        PatternStream<StationLog> patternStream = CEP.pattern(keyedStream, pattern);


        //4.获取匹配的复杂事件
        SingleOutputStreamOperator<String> result = patternStream.process(new PatternProcessFunction<StationLog, String>() {
            @Override
            public void processMatch(Map<String, List<StationLog>> map,
                                     Context context,
                                     Collector<String> collector) throws Exception {
                StationLog first = map.get("first").iterator().next();
                StationLog second = map.get("second").iterator().next();
                StationLog third = map.get("third").iterator().next();

                collector.collect("预警:基站-" + first.getSid() + "连续三次通话状态为失败。三次信息如下:\n"
                        + first + "\n"
                        + second + "\n"
                        + third);
            }
        });

        result.print();

        env.execute();
    }
}
