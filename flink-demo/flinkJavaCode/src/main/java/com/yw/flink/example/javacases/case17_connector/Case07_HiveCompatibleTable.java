package com.yw.flink.example.javacases.case17_connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink SQL - 兼容表 - 创建Hive 表并读写
 */
public class Case07_HiveCompatibleTable {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());

        //创建hive catalog
        tableEnv.executeSql("create catalog myhive with (" +
                "   'type'='hive'," +
                "   'default-database'='default'," +
                "   'hive-conf-dir'='.hiveconf'" +
                ")");

        //切换到Hive 方言
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        //使用hive catalog
        tableEnv.useCatalog("myhive");

        //编写FlinkSQL 创建Hive表
        tableEnv.executeSql("create table if not exists hive_flink_tbl (id int,name string,age int)" +
                " row format delimited fields terminated by '\t'");

        //插入数据
        tableEnv.executeSql("insert into hive_flink_tbl values (1,'zs',18),(2,'ls',19),(3,'ww',20)");

        //查询数据
        tableEnv.executeSql("select * from hive_flink_tbl ").print();

    }
}
