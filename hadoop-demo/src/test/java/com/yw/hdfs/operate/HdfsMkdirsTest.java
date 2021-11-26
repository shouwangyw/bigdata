package com.yw.hdfs.operate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.testng.Assert.assertTrue;

/**
 * @author yangwei
 */
public class HdfsMkdirsTest {
    // 简化版
    @Test
    public void mkdirsOnHdfs_simple() throws IOException {
        // 配置项
        Configuration configuration = new Configuration();
        // 设置要连接的 hdfs 集群 NameNode
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        // 获取文件系统
        FileSystem fileSystem = FileSystem.get(configuration);
        // 调用方法创建目录，若目录存在，则创建失败，返回false
        boolean result = fileSystem.mkdirs(new Path("/yw/dir1"));

        assertTrue(result);
        fileSystem.close();
    }

    // 指定目录所属用户
    @Test
    public void mkdirsOnHdfs_withUser() throws Exception {
        // 配置项
        Configuration configuration = new Configuration();
        // 获取文件系统
        FileSystem fileSystem = FileSystem.get(
                new URI("hdfs://node01:8020"), configuration, "test");
        // 调用方法创建目录，若目录存在，则创建失败，返回false
        boolean result = fileSystem.mkdirs(new Path("/yw/dir2"));

        assertTrue(result);
        fileSystem.close();
    }

    // 创建目录时，指定目录权限
    @Test
    public void mkdirsOnHdfs_withPermission() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        FsPermission fsPermission = new FsPermission(FsAction.ALL, FsAction.READ, FsAction.READ);

        boolean result = fileSystem.mkdirs(new Path("hdfs://node01:8020/yw/dir3"), fsPermission);

        assertTrue(result);
        fileSystem.close();
    }
}
