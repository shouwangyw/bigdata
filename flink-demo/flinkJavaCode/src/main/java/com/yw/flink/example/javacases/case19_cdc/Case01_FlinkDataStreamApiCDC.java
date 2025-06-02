package com.yw.flink.example.javacases.case19_cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 通过Flink CDC 监控mysql 表数据到Flink 处理
 */
public class Case01_FlinkDataStreamApiCDC {
    public static void main(String[] args) throws Exception {
        //创建 MySQL CDC Source
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("mysql_server")
                .port(3306)
                .databaseList("db1")
                .tableList("db1.tbl1,db1.tbl2")
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

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
                .setParallelism(4)
                .print();

        env.execute();
    }
}
