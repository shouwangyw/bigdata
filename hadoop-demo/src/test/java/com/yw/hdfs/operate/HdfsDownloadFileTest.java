package com.yw.hdfs.operate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

/**
 * @author yangwei
 */
public class HdfsDownloadFileTest {
    @Test
    public void downloadFile2Hdfs() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.copyToLocalFile(new Path("/yw/dir1/hello.txt"),
                new Path("/Volumes/F/MyGitHub/bigdata/hadoop-demo/src/test/resources"));

//        // 删除文件
//        fileSystem.delete()
//        // 重命名文件
//        fileSystem.rename()

        fileSystem.close();
    }
}
