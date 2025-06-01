package com.yw.flink.example.javacases.case00;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 读取Socket数据 实时统计wordcount
 */
public class Case04_SocketWordCount {
    public static void main(String[] args) throws Exception {
        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取Socket数据 hello,flink
        DataStreamSource<String> ds = env.socketTextStream("nc_server", 9999);

        //3.进行数据转换
//        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleWords = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                String[] words = line.split(",");
//                for (String word : words) {
//                    collector.collect(Tuple2.of(word, 1));
//                }
//            }
//        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleWords = ds.flatMap((String line, Collector<Tuple2<String, Integer>> collector) -> {
            String[] words = line.split(",");
            for (String word : words) {
                collector.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        tupleWords.keyBy(tp -> tp.f0).sum(1).print();

        //4.执行
        env.execute();
    }
}
