package com.yw.flink.example;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 演示Flink类型推断
 */
public class WCTest {
    public static void main(String[] args) throws Exception {
        //准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //source ,socket:hello flink
        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);
        //transformation
        SingleOutputStreamOperator<Tuple2<String, Long>> tuple2DS =
                ds.flatMap( (String s, Collector<Tuple2<String,Long>> collector) -> {
                    String[] arr = s.split(" ");
                    for (String word : arr) {
                        collector.collect(Tuple2.of(word, 1L));
                    }
                }).returns(Types.TUPLE(Types.STRING,Types.LONG));

        tuple2DS.keyBy(tp -> tp.f0).sum(1).print();

        env.execute();
    }
}
