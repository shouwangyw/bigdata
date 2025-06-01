package com.yw.flink.example.javacases.case17_connector.hive;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink SQL  - 实时数据写入Hive 分区表
 * 案例：读取Kafka 基站日志数据，写入Hive 分区表
 */
public class FlinkRTWriteHive {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());

        //设置checkpoint
        tableEnv.getConfig().getConfiguration().setLong("execution.checkpointing.interval", 5000L);

        //设置watermark自动推进
        tableEnv.getConfig().set("table.exec.source.idle-timeout", "5000");

        //创建Hive catalog
        tableEnv.executeSql("create catalog myhive with (" +
                "   'type'='hive'," +
                "   'default-database'='default'," +
                "   'hive-conf-dir'='.hiveconf'" +
                ")");

        //使用hive catalog
        tableEnv.useCatalog("myhive");

        //设置Hive 方言
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        //创建Hive 分区表 :dt hr mm ss
        tableEnv.executeSql("create table if not exists rt_hive_tbl (" +
                "   sid string," +
                "   call_out string," +
                "   call_in string," +
                "   call_type string," +
                "   call_time bigint," +
                "   duration bigint" +
                ") partitioned by (dt string,hr string,mm string,ss string) " +
                "stored as parquet tblproperties(" +
                "  'partition.time-extractor.timestamp-pattern'='$dt $hr:$mm:$ss'," +
                "  'sink.partition-commit.trigger'='partition-time'," +
                "  'sink.partition-commit.delay'='1 s'," +
                "  'sink.partition-commit.watermark-time-zone'='Asia/Shanghai'," +
                "  'sink.partition-commit.policy.kind'='metastore,success-file' " +
                ")");

        //设置Flink default 方言
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        //Flink SQL DDL 读取Kafka 中数据
        tableEnv.executeSql("create table rt_kafka_tbl (" +
                "   sid string," +
                "   call_out string," +
                "   call_in string," +
                "   call_type string," +
                "   call_time bigint," +
                "   duration bigint," +
                "   rowtime as to_timestamp_ltz(call_time,3)," +
                "   watermark for rowtime as rowtime - interval '2' seconds" +
                ") with (" +
                "   'connector'='kafka'," +
                "   'topic'='station_log-topic'," +
                "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id'='testgroup'," +
                "   'scan.startup.mode'='latest-offset'," +
                "   'format'='csv'" +
                ")");

        //将Kafka 中数据写出到Hive 分区表中
        tableEnv.executeSql("insert into rt_hive_tbl " +
                "select sid,call_out,call_in,call_type,call_time,duration," +
                "  DATE_FORMAT(rowtime,'yyyy-MM-dd') dt," +
                "  DATE_FORMAT(rowtime,'HH') hr, " +
                "  DATE_FORMAT(rowtime,'mm') mm, " +
                "  DATE_FORMAT(rowtime,'ss') ss " +
                " from rt_kafka_tbl");
    }
}
