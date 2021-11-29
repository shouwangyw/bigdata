package com.yw.hadoop.mr.p02_inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author yangwei
 */
public class KeyValueMain {
    static class KeyValueMapper extends Mapper<Text, Text, Text, LongWritable> {
        private LongWritable outvalue = new LongWritable(1);

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, outvalue);
        }
    }
    static class KeyValueReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long result = 0;
            for (LongWritable value : values) {
                result += value.get();
            }
            context.write(key, new LongWritable(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("key.value.separator.in.input.line", "@zolen@");
        Job job = Job.getInstance(conf);
        job.setJarByClass(KeyValueMain.class);

        // 第一步：读取文件，解析成key、value对
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(job, new Path(args[0]));

        // 第二步：设置mapper类
        job.setMapperClass(KeyValueMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 第三步到第六步：分区、排序、规约、分组

        // 第七步：设置reducer类
        job.setReducerClass(KeyValueReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 第八步：输出数据
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交job任务
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
