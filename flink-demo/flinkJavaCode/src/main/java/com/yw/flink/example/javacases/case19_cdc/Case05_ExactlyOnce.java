package com.yw.flink.example.javacases.case19_cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink Exactly once 测试
 */
public class Case05_ExactlyOnce {
    public static void main(String[] args) throws Exception {
        //创建 MySQL CDC Source
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("mysql_server")
                .port(3306)
                .databaseList("db3")
                .tableList("db3.test")
                .username("root")
                .password("123456")
                //指定序列化格式：读取binlog数据转换成最终字符串的格式
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        //创建Flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置checkpoint
        env.enableCheckpointing(5000);

        //设置checkpoint保存目录
        env.getCheckpointConfig().setCheckpointStorage("hdfs://mycluster/flinkcdc-cks");

        //设置checkpoint 目录清空策略
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(),"MySQL CDC Source")
                .setParallelism(4)
                .print();

        env.execute();
    }
}
