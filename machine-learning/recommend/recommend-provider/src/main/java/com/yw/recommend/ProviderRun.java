package com.yw.recommend;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.apache.dubbo.container.Main;

import java.io.IOException;

public class ProviderRun {
    // 启动方式一：
//    public static void main(String[] args) throws Exception {
//        // 创建Spring容器
//        ClassPathXmlApplicationContext ac = new ClassPathXmlApplicationContext("spring-provider.xml");
//        // 启动Spring容器
//        ac.start();
//        // 使主线程阻塞
//        System.in.read();
//    }
    // 启动方式二：要求Spring配置文件必须要放到类路径下的 META-INF/spring 目录中
    public static void main(String[] args) {
        Main.main(args);
        System.out.println("服务已经启动...");
    }
}