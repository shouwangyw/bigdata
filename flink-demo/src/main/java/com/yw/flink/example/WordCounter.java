package com.yw.flink.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Arrays;
import java.util.Objects;

/**
 * @author yangwei
 */
public class WordCounter {
    public static void main(String[] args) throws Exception {
        // 创建执行环境(批处理)
        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
        // 从文件中读取数据
        String inputPath = "/Volumes/F/MyGitHub/bigdata/flink-demo/src/main/resources/hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);
        // 对数据集进行处理，按空格分词展开，转换成(word, 1)二元组进行统计
        DataSet<Tuple2<String, Integer>> resultSet = inputDataSet
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, collector) -> {
                    // 按空格分词
                    String[] words = value.split(" ");
                    // 遍历所有word，包装成二元组输出
                    Arrays.stream(words).filter(Objects::nonNull)
                            .forEach(word -> collector.collect(new Tuple2<>(word, 1)));
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                // 按照第一个位置的word分组
                .groupBy(0)
                // 将第二个位置上的数据求和
                .sum(1);
        resultSet.print();
    }
}
