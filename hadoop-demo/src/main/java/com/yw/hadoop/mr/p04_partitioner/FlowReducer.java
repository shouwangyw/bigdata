package com.yw.hadoop.mr.p04_partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author yangwei
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, Text> {
    /**
     * 同一个手机号的数据，调用一次reduce方法
     */
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int upFlow = 0;
        int downFlow = 0;
        int upCountFlow = 0;
        int downCountFlow = 0;

        for (FlowBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            upCountFlow += value.getUpCountFlow();
            downCountFlow += value.getDownCountFlow();
        }

        context.write(key, new Text(upFlow + "\t" + downFlow + "\t" + upCountFlow + "\t" + downCountFlow));
    }
}
