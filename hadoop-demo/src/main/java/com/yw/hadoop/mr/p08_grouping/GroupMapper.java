package com.yw.hadoop.mr.p08_grouping;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yangwei
 */
public class GroupMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    private OrderBean orderBean;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        orderBean = new OrderBean();
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] slices = value.toString().split("\t");
        orderBean.setOrderId(slices[0]);
        orderBean.setPrice(Double.valueOf(slices[2]));

        context.write(orderBean, NullWritable.get());
    }
}
