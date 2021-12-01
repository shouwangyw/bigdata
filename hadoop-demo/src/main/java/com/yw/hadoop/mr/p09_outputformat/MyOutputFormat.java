package com.yw.hadoop.mr.p09_outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author yangwei
 */
public class MyOutputFormat extends FileOutputFormat<Text, NullWritable> {
    static class MyRecordWriter extends RecordWriter<Text, NullWritable> {
        private FSDataOutputStream goodStream;
        private FSDataOutputStream badStream;

        public MyRecordWriter(FSDataOutputStream goodStream, FSDataOutputStream badStream) {
            this.goodStream = goodStream;
            this.badStream = badStream;
        }

        @Override
        public void write(Text key, NullWritable value) throws IOException, InterruptedException {
            if (key.toString().split("\t")[9].equals("0")) {// 好评
                goodStream.write(key.toString().getBytes());
                goodStream.write("\r\n".getBytes());
            } else { // 中评或差评
                badStream.write(key.toString().getBytes());
                badStream.write("\r\n".getBytes());
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if (badStream != null) badStream.close();
            if (goodStream != null) goodStream.close();
        }

    }

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path goodCommentPath = new Path("/Volumes/F/MyGitHub/bigdata/hadoop-demo/src/main/resources/output/good.txt");
        Path badCommentPath = new Path("/Volumes/F/MyGitHub/bigdata/hadoop-demo/src/main/resources/output/bad.txt");

        FSDataOutputStream goodOutputStream = fs.create(goodCommentPath);
        FSDataOutputStream badOutputStream = fs.create(badCommentPath);

        return new MyRecordWriter(goodOutputStream, badOutputStream);
    }
}
