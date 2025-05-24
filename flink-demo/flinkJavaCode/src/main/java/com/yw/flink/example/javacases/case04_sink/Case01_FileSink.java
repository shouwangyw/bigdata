package com.yw.flink.example.javacases.case04_sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

/**
 * Flink 写出数据到 外部文件
 */
public class Case01_FileSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(1000);
        env.setParallelism(1);
        DataStreamSource<String> ds = env.socketTextStream("node5", 9999);

        //写出到文件
        FileSink<String> fileSink = FileSink.forRowFormat(new Path(".tmp/java-file-result"),
                        new SimpleStringEncoder<String>("UTF-8"))
                //生成桶目录的检查周期，默认1分钟
                .withBucketCheckInterval(10000)
                //设置生成文件的策略
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                //桶不活跃的间隔时长，默认1分钟
                                .withInactivityInterval(Duration.ofSeconds(30))
                                //设置文件多大后生成新的文件，默认128M
                                .withMaxPartSize(MemorySize.ofMebiBytes(1024))
                                //设置间隔多长时间生成一个新的文件，默认1分钟
                                .withRolloverInterval(Duration.ofSeconds(10))
                                .build()
                ).build();

        //写出数据到文件
        ds.sinkTo(fileSink);

        env.execute();
    }
}
