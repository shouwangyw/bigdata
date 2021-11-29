package org.apache.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @author yangwei
 */
public class FileTest {
    public static void main(String[] args) throws IOException {
        // hdfs创建目录
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        // 1. 创建目录（分析元数据管理流程）
//        fileSystem.mkdirs(new Path("/metadata"));

        // 2. 分析HDFS上传文件流程，先重点分析初始化工作
        FSDataOutputStream fsdos = fileSystem.create(new Path("/mydir"));
        // 初始化工作完成后，完成上传文件的流程
        fsdos.write(1);

        fileSystem.close();
    }
}
