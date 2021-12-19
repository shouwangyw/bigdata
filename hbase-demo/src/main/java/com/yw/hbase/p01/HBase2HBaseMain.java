package com.yw.hbase.p01;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author yangwei
 */
public class HBase2HBaseMain extends Configured implements Tool {
    private static class HBaseReadMapper extends TableMapper<Text, Put> {
        /**
         * @param key   rowKey
         * @param value rowKey 此行的数据 Result类型
         */
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            byte[] rowKeyBytes = key.get();
            // 输出数据 => 写数据 => Put
            // 构建Put对象
            Put put = new Put(rowKeyBytes);
            // 获取一行中所有的 Cell 对象
            Cell[] cells = value.rawCells();
            // 将f1 : name& age输出
            for (Cell cell : cells) {
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                if (!"f1".equals(family)) continue;
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                if (StringUtils.equalsAny(qualifier, "name", "age")) put.add(cell);
            }
            if (!put.isEmpty()) context.write(new Text(Bytes.toString(rowKeyBytes)), put);
        }
    }

    private static class HBaseWriteReducer extends TableReducer<Text, Put, ImmutableBytesWritable> {
        /**
         * 将map传输过来的数据，写入到hbase表
         */
        @Override
        protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
            ImmutableBytesWritable writable = new ImmutableBytesWritable();
            // rowKey
            writable.set(key.toString().getBytes());

            // 遍历 put 对象，并输出
            for (Put put : values) context.write(writable, put);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf());
        job.setJarByClass(HBase2HBaseMain.class);

        // Mapper
        TableMapReduceUtil.initTableMapperJob(TableName.valueOf("myuser"), new Scan(),
                HBaseReadMapper.class, Text.class, Put.class, job);
        // Reducer
        TableMapReduceUtil.initTableReducerJob("myuser2", HBaseWriteReducer.class, job);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");

        int run = ToolRunner.run(configuration, new HBase2HBaseMain(), args);
        System.exit(run);
    }
}
