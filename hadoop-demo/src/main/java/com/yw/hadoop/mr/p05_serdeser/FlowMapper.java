package com.yw.hadoop.mr.p05_serdeser;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yangwei
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private FlowBean flowBean;
    private Text text;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.flowBean = new FlowBean();
        this.text = new Text();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] slices = value.toString().split("\t");

        text.set(slices[1]);

        flowBean.setUpPackNum(Integer.parseInt(slices[6]));
        flowBean.setDownPackNum(Integer.parseInt(slices[7]));
        flowBean.setUpPayLoad(Integer.parseInt(slices[8]));
        flowBean.setDownPayLoad(Integer.parseInt(slices[9]));

        context.write(text, flowBean);
    }
}
