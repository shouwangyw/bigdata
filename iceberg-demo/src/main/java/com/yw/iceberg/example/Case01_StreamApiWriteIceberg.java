package com.yw.iceberg.example;

import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;

import java.util.Map;

/**
 * 使用 Flink 的DataStream API向iceberg表写入数据
 * @author yangwei
 */
public class Case01_StreamApiWriteIceberg {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1. 必须设置checkpoint，Flink向iceberg中写入数据时当checkpoint发生后，才会commit数据
        env.enableCheckpointing(5000);

        // 2. 读取Kafka中的topic数据
        DeserializationSchema simpleStringSchema = new SimpleStringSchema();
        KafkaSource<String> source = KafkaSource.builder()
                .setBootstrapServers("node1:9092,node2:9092,node3:9092")
                .setTopics("flink-iceberg-topic")
                .setGroupId("group-flink-iceberg")
                .setValueOnlyDeserializer(simpleStringSchema)
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();
        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 3. 对数据进行处理，包装成RowData对象，方便保存到iceberg表中
        SingleOutputStreamOperator<RowData> dataStream = kafkaSource.map((MapFunction<String, RowData>) s -> {
            System.out.println("s = " + s);
            String[] slices = s.split(",");
            GenericRowData row = new GenericRowData(4);
            row.setField(0, Integer.valueOf(slices[0]));
            row.setField(1, StringData.fromString(slices[1]));
            row.setField(2, Integer.valueOf(slices[2]));
            row.setField(3, StringData.fromString(slices[3]));
            return row;
        });

        // 4. 创建Hadoop配置，Catalog配置和表的Schema，方便后续向路径写数据时可以找到对应的表
        Configuration hadoopConf = new Configuration();
        Catalog catalog = new HadoopCatalog(hadoopConf, "hdfs://mycluster/flink_iceberg");
        // 配置iceberg库名和表名
        TableIdentifier tableName = TableIdentifier.of("default", "flink_iceberg_tbl");
        // 配置iceberg表schema
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "age", Types.IntegerType.get()),
                Types.NestedField.required(4, "loc", Types.StringType.get())
        );
        // 如果有分区，那么指定对应分区。这里loc列为分区列，可以指定 unpartitioned 方法不设置表分区
//        PartitionSpec spec = PartitionSpec.unpartitioned();
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("loc").build();
        // 指定iceberg表数据格式化为 parquet 文件存储
        Map<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name());
        Table table;
        // 通过catalog判断表是否存在，不存在就创建，存在就加载
        if (!catalog.tableExists(tableName)) {
            table = catalog.createTable(tableName, schema, spec, props);
        } else {
            table = catalog.loadTable(tableName);
        }

        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://mycluster/flink_iceberg/default/flink_iceberg_tbl", hadoopConf);

        // 5. 通过DataStream API 向 iceberg 表中写入数据
        FlinkSink.forRowData(dataStream)
                .table(table)
                .tableLoader(tableLoader)
                // 默认为false 追加数据，如果设置为true就是覆盖数据
                .overwrite(false)
                .append();

        env.execute("DataStream API write data to iceberg");
    }
}
