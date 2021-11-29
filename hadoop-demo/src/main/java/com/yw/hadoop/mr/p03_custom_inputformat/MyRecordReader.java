package com.yw.hadoop.mr.p03_custom_inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author yangwei
 */
public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {
    /**
     * 要读取的分片
     */
    private FileSplit fileSplit;
    private Configuration configuration;
    /**
     * 当前的value值
     */
    private BytesWritable bytesWritable;
    /**
     * 标记一下分片有没有被读取，默认false
     */
    private boolean flag = false;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) split;
        this.configuration = context.getConfiguration();
        this.bytesWritable = new BytesWritable();
    }

    /**
     * RecordReader 读取分片时，先判断是否有下一个kv对，根据flag判断
     * 如果有，则一次性的将文件内容全部读出
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!flag) {
            int length = (int) fileSplit.getLength();
            byte[] splitContent = new byte[length];
            // 读取分片内容
            Path path = fileSplit.getPath();
            FileSystem fileSystem = path.getFileSystem(configuration);
            FSDataInputStream fsdis = fileSystem.open(path);
            // split 内容写入 splitContent
            IOUtils.readFully(fsdis, splitContent, 0, length);
            // 当前value值
            bytesWritable.set(splitContent, 0, length);
            flag = true;

            IOUtils.closeStream(fsdis);
//            fileSystem.close();
            return true;
        }
        return false;
    }

    /**
     * 获取当前键值对的建
     */
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    /**
     * 获取当前键值对的值
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    /**
     * 读取分片的进度
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return flag ? 1.0f : 0.0f;
    }

    /**
     * 释放资源
     */
    @Override
    public void close() throws IOException {

    }
}
