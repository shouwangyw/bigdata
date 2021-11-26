package com.yw.hdfs.operate;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

/**
 * @author yangwei
 */
public class HdfsUploadFileTest {
    @Test
    public void uploadFile2Hdfs() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.copyFromLocalFile(new Path("/Volumes/F/MyGitHub/bigdata/hadoop-demo/src/test/resources/hello.txt"),
                new Path("/yw/dir1"));
        fileSystem.close();
    }

    @Test
    public void putFile2Hdfs() throws Exception {
        // 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node01:8020"), configuration);
        // 创建输入流，不需要加 file:///，否则报错
        FileInputStream fis = new FileInputStream(new File("/Volumes/F/MyGitHub/bigdata/hadoop-demo/src/test/resources/hello.txt"));
        // 创建输出流，父目录不存在，会自动创建
        FSDataOutputStream fos = fs.create(new Path("/yw/dir2/hello.txt"));
        // 流对拷
        IOUtils.copy(fis, fos); // org.apache.commons.io.IOUtils
        // 关闭资源
        IOUtils.closeQuietly(fis);
        IOUtils.closeQuietly(fos);
        fs.close();
    }

    /**
     * 小文件合并：读取所有本地小文件，写入到hdfs的大文件里面去
     */
    @Test
    public void mergeFile() throws Exception {
        // 获取分布式文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node01:8020"), configuration, "hadoop");
        FSDataOutputStream fsdos = fs.create(new Path("/yw/dir3/big.txt"));

        LocalFileSystem lfs = FileSystem.getLocal(configuration);
        FileStatus[] fileStatuses = lfs.listStatus(new Path("/Volumes/F/MyGitHub/bigdata/hadoop-demo/src/test/resources/"));
        for (FileStatus fileStatus : fileStatuses) {
            // 获取每一个本地文件路径
            Path path = fileStatus.getPath();
            // 读取本地小文件
            FSDataInputStream fsdis = lfs.open(path);
            IOUtils.copy(fsdis, fsdos);
            IOUtils.closeQuietly(fsdis);
        }
        IOUtils.closeQuietly(fsdos);
        lfs.close();
        fs.close();
    }
}
