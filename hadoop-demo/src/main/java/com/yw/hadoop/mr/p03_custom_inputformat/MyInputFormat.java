package com.yw.hadoop.mr.p03_custom_inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @author yangwei
 */
public class MyInputFormat extends FileInputFormat<NullWritable, BytesWritable> {
    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        MyRecordReader recordReader = new MyRecordReader();
        recordReader.initialize(split, context);
        return recordReader;
    }

    /**
     * 注意这个方法，决定我们的文件是否可以切分，如果不可切分，直接返回false
     * 到时候读取一个文件的数据的时候，一次性将此文件全部内容都读取出来
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
