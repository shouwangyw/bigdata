package com.yw.iceberg.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;

/**
 * 使用Flink DataStream API 批量/实时 读取 iceberg 表中数据
 * @author yangwei
 */
public class Case02_StreamApiReadIceberg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. 配置 TableLoader
        Configuration hadoopConf = new Configuration();
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://mycluster/flink_iceberg/default/flink_iceberg_tbl", hadoopConf);

        // 2. 从iceberg中读取全量/增量数据
        DataStream<RowData> batchData = FlinkSource.forRowData().env(env)
                .tableLoader(tableLoader)
                // 基于某个快照实时增量读取数据，快照需要从元数据中获取
                .startSnapshotId(6676939247162515637L)
                // 默认为false，整批次读取
//                .streaming(false)
                // 设置为true时为流式读取
                .streaming(true)
                .build();

        batchData.map((MapFunction<RowData, String>) rowData -> {
            int id = rowData.getInt(0);
            String name = rowData.getString(1).toString();
            int age = rowData.getInt(2);
            String loc = rowData.getString(3).toString();
            return id + "," + name + "," + age + "," + loc;
        }).print();

        env.execute("DataStream Api Read Data From Iceberg");
    }
}
