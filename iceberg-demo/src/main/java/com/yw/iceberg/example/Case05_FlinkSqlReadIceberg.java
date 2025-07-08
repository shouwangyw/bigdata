package com.yw.iceberg.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 使用Flink SQL API批量查询iceberg表中的数据
 * @author yangwei
 */
public class Case05_FlinkSqlReadIceberg {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.enableCheckpointing(1000);

        // 1. 创建catalog
        tableEnv.executeSql("create catalog hadoop_iceberg with (" +
                "'type'='iceberg'," +
                "'catalog-type'='hadoop'," +
                "'warehouse'='hdfs://mycluster/flink_iceberg')");

        // 2. 批量读取表数据
        TableResult tableResult = tableEnv.executeSql("select * from hadoop_iceberg.iceberg_db.flink_iceberg_tbl1");

        tableResult.print();
        /* 结果输出如下
        +----+-------------+--------------------------------+-------------+--------------------------------+
        | op |          id |                           name |         age |                            loc |
        +----+-------------+--------------------------------+-------------+--------------------------------+
        | +I |           3 |                             ww |          20 |                      guangzhou |
        | +I |           1 |                             zs |          18 |                        beijing |
        | +I |           2 |                             ls |          19 |                       shanghai |
        +----+-------------+--------------------------------+-------------+--------------------------------+
         */
    }
}
