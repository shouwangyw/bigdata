package com.yw.hadoop.mr.p06_second_sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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
        String[] slices = value.toString().split("\t");

        flowSortBean.setPhone(slices[0]);
        flowSortBean.setUpPackNum(Integer.parseInt(slices[1]));
        flowSortBean.setDownPackNum(Integer.parseInt(slices[2]));
        flowSortBean.setUpPayLoad(Integer.parseInt(slices[3]));
        flowSortBean.setDownPayLoad(Integer.parseInt(slices[4]));

        context.write(flowSortBean, NullWritable.get());
    }
}
