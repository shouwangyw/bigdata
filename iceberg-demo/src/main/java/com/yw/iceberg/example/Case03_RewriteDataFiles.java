package com.yw.iceberg.example;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.actions.Actions;
import org.apache.iceberg.hadoop.HadoopCatalog;

/**
 * 合并 data files
 *
 * @author yangwei
 */
public class Case03_RewriteDataFiles {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 1. 配置TableLoader
        Configuration hadoopConf = new Configuration();

        // 2. 创建 Hadoop 配置、Catalog 配置和表的 Schema，方便后续向路径写数据时可以找到对应的表
        HadoopCatalog catalog = new HadoopCatalog(hadoopConf, "hdfs://mycluster/flink_iceberg");

        // 3. 配置iceberg 库名和表名并加载数据
        TableIdentifier tableIdentifier = TableIdentifier.of("default", "flink_iceberg_tbl");
        Table table = catalog.loadTable(tableIdentifier);

        // 4. 合并data files小文件
        RewriteDataFilesActionResult result = Actions.forTable(table)
                .rewriteDataFiles()
                // 默认512M，可以手动通过以下指定合并文件大小，与Spark中一样
                .targetSizeInBytes(536870912L)
                .execute();
    }
}
