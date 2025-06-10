package com.yw.flink.example.javacases.case00;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Flink 批处理 WordCount
 *
 * @author yangwei
 */
public class Case01_BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 准备Flink运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取数据文件
        DataSource<String> lineDs = env.readTextFile(".data/words.txt");

        // 3. 对数据进行切分单词，并转成Tuple2 KV 类型
        FlatMapOperator<String, Tuple2<String, Long>> kvWordsDs =
                lineDs.flatMap((String line, Collector<Tuple2<String, Long>> collector) -> {
                    for (String word : line.split(" ")) {
                        collector.collect(Tuple2.of(word, 1L));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4. 进行数据分组，计数，打印结果
        kvWordsDs.groupBy(0).sum(1).print();
    }
}
