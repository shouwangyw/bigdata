package com.yw.flink.example.javacases.case19_cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.ObjectPath;

/**
 * MySQL CDC 监控MySQL 无主键表 数据
 */
public class Case03_DsNonPrimaryKey {
    public static void main(String[] args) throws Exception {
        //创建 MySQL CDC Source
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("mysql_server")
                .port(3306)
                .databaseList("db2")
                .tableList("db2.tbl")
                .username("root")
                .password("123456")
                //指定序列化格式：读取binlog数据转换成最终字符串的格式
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                //设置chunk key column
                .chunkKeyColumn(new ObjectPath("db2", "tbl"), "id")
                .build();

        //创建Flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置checkpoint
        env.enableCheckpointing(5000);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
                .setParallelism(4)
                .print();

        env.execute();
    }
}
