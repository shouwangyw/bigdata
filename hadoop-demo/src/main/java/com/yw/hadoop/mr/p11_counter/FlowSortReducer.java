package com.yw.hadoop.mr.p11_counter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author yangwei
 */
public class FlowSortReducer extends Reducer<FlowSortBean, NullWritable, FlowSortBean, NullWritable> {

    static enum Counter {
        REDUCE_INPUT_RECORDS
    }

    @Override
    protected void reduce(FlowSortBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.getCounter(Counter.REDUCE_INPUT_RECORDS).increment(1L);

        // 经过排序后，直接输出
        context.write(key, NullWritable.get());
    }
}
