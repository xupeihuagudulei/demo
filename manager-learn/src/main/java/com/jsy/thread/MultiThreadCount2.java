package com.jsy.thread;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 多线程打印数字
 *
 * @Author: jsy
 * @Date: 2021/9/5 17:38
 */

public class MultiThreadCount2 {
    static CountDownLatch cdl;
    static AtomicInteger ai = new AtomicInteger(0);

    public static void main(String[] args) throws InterruptedException {
        Integer num = 10000 * 10;
        Integer threadNum = 100;
        if (args.length == 1) {
            num = Integer.parseInt(args[0]);
        } else if (args.length == 2) {
            num = Integer.parseInt(args[0]);
            threadNum = Integer.parseInt(args[1]);
        }
        System.out.println("可输入第一个参数打印总数，第二个参数线程数");
        System.out.println("打印总数：" + num + ",线程数+" + threadNum);



        LocalDateTime start = LocalDateTime.now();
        // 100个线程
        ExecutorService exec = Executors.newFixedThreadPool(threadNum);

        // 打印
        cdl = new CountDownLatch(num);
        for (int i = 0; i < num; i++) {
            exec.execute(() -> {
                int currentNum = ai.getAndIncrement();
                // 一千万采样一次
                if (currentNum % (1000 * 10000) == 0) {
                    System.out.println(Thread.currentThread().getName() + ":" + currentNum);
                }
                cdl.countDown();
            });
        }
        cdl.await();
        System.out.println("共输出次数:" + ai.get());

        System.out.println("cost seconds is:" + Duration.between(start, LocalDateTime.now()).getSeconds());
        exec.shutdown();

    }
}