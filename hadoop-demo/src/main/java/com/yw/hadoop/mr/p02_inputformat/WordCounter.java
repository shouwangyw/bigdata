package com.yw.hadoop.mr.p02_inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author yangwei
 */
public class WordCounter extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // 获取Job对象，组装我们的八个步骤，每一个步骤都是一个class类
        Configuration conf = super.getConf();

        Job job = Job.getInstance(conf, WordCounter.class.getSimpleName());

        // 判断输出路径是否存在，如果存在则删除
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        // 实际工作中，程序运行完之后一般都是打包到集群上面去运行，打成一个 jar 包
        // 如果要打包到集群上面运行，必须添加以下设置
        job.setJarByClass(WordCounter.class);

        // 第一步：读取文件，解析成key、value对，k1: 行偏移量，v1: 一行文本内容
//        job.setInputFormatClass(TextInputFormat.class);
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 虚拟存储切片最大值设置4m  设置每个切片处理数据量为4M
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
        // 指定我们去哪一个路径读取文件
        CombineTextInputFormat.addInputPath(job, new Path(args[0]));

        // 第二步：自定义map逻辑，接收k1、v1，转换成新的k2、v2输出
        job.setMapperClass(MyMapper.class);
        // 设置map阶段输出的key、value的类型，其实就是k2、v2的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 第三步到六步：分区、排序、规约、分组。。。省略

        // 第七步：自定义reduce逻辑
        job.setReducerClass(MyReducer.class);
        // 设置 key3、value3的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 第八步：输出k3、v3，进行保存
        job.setOutputFormatClass(TextOutputFormat.class);
        // 一定要注意，输出路径是需要不存在的，如果存在就报错
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(Integer.parseInt(args[2]));

        // 提交job任务
        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        // 提交run方法之后，得到一个程序的退出状态码
        int run = ToolRunner.run(configuration, new WordCounter(), args);
        // 根据我们的程序的退出状态码，退出整个进程
        System.exit(run);
    }

    static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int result = 0;
            for (IntWritable value : values) {
                result += value.get();
            }
            IntWritable intWritable = new IntWritable(result);
            context.write(key, intWritable);
        }
    }

    static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private IntWritable intWritable = new IntWritable(1);
        private Text text = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(",");

            for (String word : words) {
                text.set(word);
                context.write(text, intWritable);
            }
        }
    }
}
