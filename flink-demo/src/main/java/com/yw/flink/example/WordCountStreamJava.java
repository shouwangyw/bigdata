package com.yw.flink.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.Objects;

/**
 * 通过java开发flink流处理作业
 */
public class WordCountStreamJava {
    public static void main(String[] args) throws Exception {
        // 1. 构建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 从socket获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("node01", 9999);

        // 3. 对数据进行处理
        DataStream<Tuple2<String, Integer>> resultStream = streamSource
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (line, collector) -> {
                    String[] words = line.split(" ");
                    Arrays.stream(words).filter(Objects::nonNull)
                            .forEach(word -> collector.collect(new Tuple2<>(word, 1)));
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) tuple2 -> tuple2.f0)
                .sum(1);
        // 4. 打印输出, sink
        resultStream.print();

        // 5. 开启任务
        env.execute("WordCountStreamJava");
    }
}
