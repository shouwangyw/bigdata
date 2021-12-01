package com.yw.hadoop.mr.p11_counter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yangwei
 */
public class FlowSortMapper extends Mapper<LongWritable, Text, FlowSortBean, NullWritable> {
    private FlowSortBean flowSortBean;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        flowSortBean = new FlowSortBean();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 自定义我们的计数器，这里实现了统计 map 输入数据的条数
        Counter counter = context.getCounter("MR_COUNT", "MapRecordCounter");
        counter.increment(1L);

        String[] slices = value.toString().split("\t");

        flowSortBean.setPhone(slices[0]);
        flowSortBean.setUpPackNum(Integer.parseInt(slices[1]));
        flowSortBean.setDownPackNum(Integer.parseInt(slices[2]));
        flowSortBean.setUpPayLoad(Integer.parseInt(slices[3]));
        flowSortBean.setDownPayLoad(Integer.parseInt(slices[4]));

        context.write(flowSortBean, NullWritable.get());
    }
}
