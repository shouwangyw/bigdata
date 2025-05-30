package com.yw.flink.example.javacases.case14_flinksql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/**
 * Flink DataStream转换成Table - tableEnvironment.fromChangelogStream(...)
 */
public class Case08_FromChangelogStream {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建TableEnvironment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //设置并行度为1
        env.setParallelism(1);

        //准备数据流
        DataStreamSource<Row> dataStream = env.fromElements(
                Row.ofKind(RowKind.INSERT, "zs", 18),
                Row.ofKind(RowKind.INSERT, "ls", 19),
                Row.ofKind(RowKind.INSERT, "ww", 20),
                Row.ofKind(RowKind.UPDATE_BEFORE, "zs", 18),
                Row.ofKind(RowKind.UPDATE_AFTER, "zs", 21),
                Row.ofKind(RowKind.DELETE, "ls", 19),
                Row.ofKind(RowKind.INSERT, "xx", 100)
        );

        Table result = tableEnv.fromChangelogStream(
                dataStream,
                Schema.newBuilder()
                        .primaryKey("f0")
                        .build(),
                ChangelogMode.all()
        );

        result.printSchema();
        result.execute().print();
    }
}
