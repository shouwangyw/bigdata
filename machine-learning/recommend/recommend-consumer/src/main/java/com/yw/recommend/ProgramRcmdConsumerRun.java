package com.yw.recommend;

import com.yw.recommend.service.ProgramRcmdService;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author yangwei
 */
public class ProgramRcmdConsumerRun {
    public static void main(String[] args) throws InterruptedException {
        //测试常规服务
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("spring-consumer.xml");
        context.start();
        ProgramRcmdService programRcmdService = context.getBean(ProgramRcmdService.class);
        while (true){
            List<String> recommendList = programRcmdService.getProgramRcmdList("cd00000000");
            for (String item : recommendList) {
                System.out.println(item);
            }
            TimeUnit.SECONDS.sleep(10);
        }
    }
}
