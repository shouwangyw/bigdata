package com.yw.iceberg.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 使用Flink SQL API实时查询iceberg表中的数据
 * @author yangwei
 */
public class Case06_FlinkSqlStreamingReadIceberg {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.enableCheckpointing(1000);

        Configuration conf = tableEnv.getConfig().getConfiguration();
        // 支持SQL语法中的OPTIONS选项
        conf.setBoolean("table.dynamic-table-options.enabled", true);

        // 1. 创建catalog
        tableEnv.executeSql("create catalog hadoop_iceberg with (" +
                "'type'='iceberg'," +
                "'catalog-type'='hadoop'," +
                "'warehouse'='hdfs://mycluster/flink_iceberg')");

        // 2. 从 Iceberg 表当前快照读取所有数据，并继续增量读取数据
        // streaming 指定为 true 支持实时读取数据，monitor-interval 监控数据的间隔，默认 1s
        TableResult tableResult = tableEnv.executeSql("select * from hadoop_iceberg.iceberg_db.flink_iceberg_tbl1" +
                "/*+ OPTIONS('streaming'='true','monitor-interval'='1s')*/");
        tableResult.print();

//        // 2.从 Iceberg 指定的快照继续实时读取数据，快照 ID 从对应的元数据中获取
//        // start-snapshot-id :快照 ID
//        TableResult tableResult2 = tableEnv.executeSql("select * from hadoop_iceberg.iceberg_db.flink_iceberg_tbl1" +
//                "/*+ OPTIONS('streaming'='true', 'monitor-interval'='1s', 'start-snapshot-id'='2624695081150890097')*/");
//
//        tableResult2.print();
    }
}
