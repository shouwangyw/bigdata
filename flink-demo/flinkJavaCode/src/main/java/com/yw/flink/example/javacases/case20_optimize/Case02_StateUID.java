package com.yw.flink.example.javacases.case20_optimize;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink 算子设置uid ,状态保存及恢复
 * <p>
 * 1.配置Flink savepoint
 * 2.代码打包，提交集群执行
 * 3.执行savepoint，保存状态
 * 4.根据保存的savepoint ,恢复状态
 */
public class Case02_StateUID {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取socket数据做wordcount ,a,b,c
        SingleOutputStreamOperator<String> lines = env.socketTextStream("nc_server", 9999).uid("socket-source");

        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2 =
                lines.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
                    String[] words = s.split(",");
                    for (String word : words) {
                        collector.collect(new Tuple2<>(word, 1));
                    }
                }).uid("flatmap");

        SingleOutputStreamOperator<Tuple2<String, Integer>> result =
                tuple2.keyBy((KeySelector<Tuple2<String, Integer>, String>) stringIntegerTuple2 -> stringIntegerTuple2.f0)
                        .sum(1).uid("sum");

        result.print().uid("print");

        env.execute();
    }

}
