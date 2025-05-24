package com.yw.flink.example.javacases.case01_source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink 读取HDFS 文件数据
 */
public class Case01_FileSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<String> result = env.readTextFile("hdfs://mycluster/flinkdata/a.txt");
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("hdfs://mycluster/flinkdata/a.txt")).build();

        //使用source
        DataStreamSource<String> result = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source");

        result.print();
        env.execute();
    }
}
