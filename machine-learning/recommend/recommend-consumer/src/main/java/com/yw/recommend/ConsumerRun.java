package com.yw.recommend;

import com.yw.recommend.service.DemoService;
import com.yw.recommend.service.RcmdService;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.concurrent.TimeUnit;

public class ConsumerRun {
    public static void main(String[] args) throws InterruptedException {
        ApplicationContext ac = new ClassPathXmlApplicationContext("spring-consumer.xml");
        DemoService service = ac.getBean(DemoService.class);
        service.sayName("China");

        RcmdService rcmdService = ac.getBean(RcmdService.class);
        while (true){
            System.out.println(rcmdService.getRcmdList("0b2ObVoIrENRNjEt"));

            TimeUnit.SECONDS.sleep(10);
        }
    }
}
