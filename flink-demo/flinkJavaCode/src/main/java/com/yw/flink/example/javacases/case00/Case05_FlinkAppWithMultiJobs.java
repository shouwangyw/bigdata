package com.yw.flink.example.javacases.case00;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 一个Flink Application 包含多个job
 */
public class Case05_FlinkAppWithMultiJobs {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds1 = env.socketTextStream("node5", 8888);
        DataStreamSource<String> ds2 = env.socketTextStream("node5", 9999);

        //针对ds1进行处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleWords =
                ds1.flatMap((String line, Collector<Tuple2<String, Integer>> collector) -> {
                    String[] words = line.split(",");
                    for (String word : words) {
                        collector.collect(Tuple2.of(word, 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT));

        tupleWords.print();
        env.executeAsync("first job");

        //针对ds2进行处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleWords2 =
                ds2.flatMap((String line, Collector<Tuple2<String, Integer>> collector) -> {
                    String[] words = line.split(",");
                    for (String word : words) {
                        collector.collect(Tuple2.of(word, 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT));

        tupleWords2.keyBy(tp -> tp.f0).sum(1).print();

        env.executeAsync("second job");


    }
}
