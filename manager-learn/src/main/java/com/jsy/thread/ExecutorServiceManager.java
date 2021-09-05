package com.jsy.thread;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 线程池统一管理
 *
 * @Author: jsy
 * @Date: 2020/9/18 0:22
 */
@Component
public class ExecutorServiceManager {

    // 注入Environment可以读取配置文件
    @Autowired
    private Environment environment;

    private ThreadPoolExecutor threadPoolExecutor;

    private ScheduledExecutorService scheduledExecutorService;

    @PostConstruct
    private void init() {
        threadPoolExecutor = new ThreadPoolExecutor(100,
                100, 100, TimeUnit.DAYS, new LinkedBlockingQueue<>(1000)
        );

        // scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NameThreadFactory("sss"));
    }
}

class NameThreadFactory {

    private String threadName;
    public String NameThreadFactory(String name){
        this.threadName += name;
        return this.threadName;
    }
}