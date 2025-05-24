package com.yw.flink.example.javacases.case00;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink DataStream Batch WordCount
 *
 * @author yangwei
 */
public class Case03_StreamBatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 准备Flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置批运行模式
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
//        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 2. 读取数据文件
        DataStreamSource<String> lineDs = env.readTextFile(".data/words.txt");

        // 3. 对数据进行切分单词，并转成Tuple2 KV 类型
        SingleOutputStreamOperator<Tuple2<String, Long>> kvWordsDs =
                lineDs.flatMap((String line, Collector<Tuple2<String, Long>> collector) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        collector.collect(Tuple2.of(word, 1L));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4. 进行数据分组，计数，打印结果
//        kvWordsDs.keyBy(0).sum(1).print();
        kvWordsDs.keyBy(tp -> tp.f0).sum(1).print();

        // 5. 流式计算中需要最后执行execute方法
        env.execute();
    }
}
