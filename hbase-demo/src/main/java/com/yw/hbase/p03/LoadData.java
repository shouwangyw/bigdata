package com.yw.hbase.p03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;

/**
 * @author yangwei
 */
public class LoadData {

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");

        BulkLoadHFiles load = BulkLoadHFiles.create(configuration);
        load.bulkLoad(TableName.valueOf("myuser2"),
                new Path("hdfs://node01:8020/hbase/out_hfile"));
    }
}