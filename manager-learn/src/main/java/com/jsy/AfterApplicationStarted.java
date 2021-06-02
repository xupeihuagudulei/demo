package com.jsy;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * @Author: jsy
 * @Date: 2021/5/30 17:09
 */
@Component
public class AfterApplicationStarted implements ApplicationRunner {

    @Override
    public void run(ApplicationArguments args) throws Exception {
        System.out.println("ssssssss");
    }
}
