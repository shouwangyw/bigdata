package com.yw.hadoop.mr.p01_word_count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author yangwei
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * 第三步：分区，相同的 key 的数据发送到同一个reduce里面去，相同key合并，value形成一个集合
     * 比如：(hadoop,1)
     *      (hive,1)
     *      (hadoop,1)
     *      (hive,1)
     *      (hadoop,1)
     *      (hive,1)
     *      (hadoop,1)
     *      ==>> hadoop, Iterable<IntWritable>(1,1,1,1) ==>> 调用一次 reduce()
     *      ==>> hive, Iterable<IntWritable>(1,1,1) ==>> 调用一次 reduce()
     * 继承reducer之后，覆写reduce方法
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int result = 0;
        for (IntWritable value : values) {
            // 将我们的结果进行累加
            result += value.get();
        }
        // 继续输出我们的数据
        IntWritable intWritable = new IntWritable(result);
        // 将我们的数据输出
        context.write(key, intWritable);
    }
}
