package com.yw.hdfs.operate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * @author yangwei
 */
public class HdfsViewFileTest {
    @Test
    public void uploadFile2Hdfs() throws Exception {
        // 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node01:8020"), configuration);
        // 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/yw"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();
            // 输出文件详情
            // 文件名称
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            // 分组
            System.out.println(status.getGroup());
            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
        }
        // 关闭资源
        fs.close();
    }
}
