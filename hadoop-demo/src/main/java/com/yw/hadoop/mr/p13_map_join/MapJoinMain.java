package com.yw.hadoop.mr.p13_map_join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * @author yangwei
 */
public class MapJoinMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // 分布式缓存的hdfs路径
        URI uri = new URI("hdfs://node01:8020/cache/pdts.txt");

        Job job = Job.getInstance(super.getConf(), MapJoinMain.class.getSimpleName());
        job.setJarByClass(MapJoinMain.class);
        // 添加缓存文件
        job.addCacheFile(uri);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(2);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new MapJoinMain(), args);
        System.exit(run);
    }
}
