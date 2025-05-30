package com.yw.flink.example.javacases.case14_flinksql;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink Table API 和SQL 快速上手
 */
public class Case00_TableApiAndSQL {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.创建环境
//        EnvironmentSettings settings = EnvironmentSettings.newInstance()
//                .inStreamingMode()
//                .build();
//        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //2.设置并行度为1
        tableEnv.getConfig().getConfiguration().setString("parallelism.default","1");

        //3.使用Table API 读取数据源，创建Source Table - datagen
        tableEnv.createTable("SourceTable",
                TableDescriptor.forConnector("datagen")
                        //定义表结构
                        .schema(Schema.newBuilder()
                                .column("f0", DataTypes.STRING())
                                .build())
                        //产生数据的速度
                        .option(DataGenConnectorOptions.ROWS_PER_SECOND,10L)
                        .build()
                );

        //4.使用SQL编程方式创建Sink Table
        tableEnv.executeSql("" +
                "CREATE Table SinkTable (" +
                "   f0 string" +
                ") with (" +
                "   'connector'='print'" +
                ")");

        //5.使用Table API 将表 SourceTable 查询出来
        Table table1 = tableEnv.from("SourceTable");

        //6.使用SQL 查询表 SourceTable 数据
        Table table2 = tableEnv.sqlQuery("select * from SourceTable");

        //7.通过Table API 将结果输出到SinkTable表
//        table1.executeInsert("SinkTable");

        //8.通过SQL 编程将结果输出到SinkTable表
        tableEnv.executeSql("insert into SinkTable select f0 from SourceTable");
    }
}
