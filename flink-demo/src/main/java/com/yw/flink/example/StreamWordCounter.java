package com.yw.flink.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author yangwei
 */
public class StreamWordCounter {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(8);

//        // 从文件中读取数据
//        String inputPath = "/Volumes/F/MyGitHub/bigdata/flink-demo/src/main/resources/hello.txt";
//        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // 用 parameter tool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // 从socket文本流读取数据
        // $ nc -lk 7777
        // hello world
        DataStream<String> inputDataStream = env.socketTextStream(host, port);

        // 基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, collector) -> {
                    // 按空格分词
                    String[] words = value.split(" ");
                    // 遍历所有word，包装成二元组输出
                    Arrays.stream(words).filter(Objects::nonNull)
                            .forEach(word -> collector.collect(new Tuple2<>(word, 1)));
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
//                .slotSharingGroup("green")
                .keyBy(0).sum(1)
                // 每一步也都可以设置并行度，即每一步否可以使用多线程去执行
                .setParallelism(2)
                // 每一步也都可以设置slot的共享组
//                .slotSharingGroup("red")
                ;

//        resultStream.print();
        // 不设置共享组，则和上一步共享组一致
        resultStream.print().setParallelism(1);
        // 执行任务
        env.execute();
    }

    public List<Integer> getRow(int rowIndex) {
        int n = rowIndex + 1;
        int[][] arr = new int[2][n];
        for (int i = 0; i < n; i++) {
            int idx = i % 2, pre = idx == 1 ? 0 : 1;
            arr[idx][0] = 1;
            for (int j = 1; j <= i; j++) {
                arr[idx][j] = arr[pre][j] + arr[pre][j - 1];
            }
        }

        return IntStream.of(arr[(n - 1) % 2]).boxed().collect(Collectors.toList());
    }
}
