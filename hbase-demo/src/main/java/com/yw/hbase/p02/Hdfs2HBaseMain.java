package com.yw.hbase.p02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author yangwei
 */
public class Hdfs2HBaseMain extends Configured implements Tool {
    private static class HdfsReadMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 数据原样输出
            context.write(value, NullWritable.get());
        }
    }

    private static class HBaseWriteReducer extends TableReducer<Text, NullWritable, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            String[] slices = key.toString().split("\t");
            Put put = new Put(Bytes.toBytes(slices[0]));
            put.addColumn("f1".getBytes(), "name".getBytes(), slices[1].getBytes());
            put.addColumn("f1".getBytes(), "age".getBytes(), slices[2].getBytes());

            context.write(new ImmutableBytesWritable(Bytes.toBytes(slices[0])), put);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf());
        job.setJarByClass(Hdfs2HBaseMain.class);

        // 输入文件路径
        FileInputFormat.addInputPath(job, new Path("hdfs://node01:8020/hbase/input"));

        job.setMapperClass(HdfsReadMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 指定输出到hbase表名
        TableMapReduceUtil.initTableReducerJob("myuser2", HBaseWriteReducer.class, job);

        // 设置reduce个数
        job.setNumReduceTasks(1);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");

        int run = ToolRunner.run(configuration, new Hdfs2HBaseMain(), args);
        System.exit(run);
    }
}
