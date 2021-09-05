package com.jsy.thread;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 多线程计数
 *
 * @Author: jsy
 * @Date: 2020/12/17 22:33
 */
public class MultiThreadCount {

    private static AtomicLong count = new AtomicLong(0);

    private static ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(100,
            100, 100, TimeUnit.DAYS, new LinkedBlockingQueue<>(1000));

    public static void main(String[] args) {
        executeTask();
    }

    private static void executeTask() {

        for (int i = 0; i < 50; i++) {
            threadPoolExecutor.execute(() -> {
                long id = count.addAndGet(1L);
                System.out.println("------------>>>" + id);

            });
        }
    }
}

