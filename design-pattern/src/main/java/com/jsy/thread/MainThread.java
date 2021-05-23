package com.jsy.thread;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 多线程demo
 * <p>
 * 在《阿里巴巴Java开发手册》中有一条
 * <p>
 * 【强制】线程池不允许使用 Executors 去创建，而是通过 ThreadPoolExecutor 的方式，这样的处理方式让写的同学更加明确线程池的运行规则，规避资源耗尽的风险。
 * <p>
 * Executors 返回的线程池对象的弊端如下:
 * FixedThreadPool 和 SingleThreadPool : 允许的请求队列长度为 Integer.MAX_VALUE，可能会堆积大量的请求，从而导致 OOM。
 * CachedThreadPool 和 ScheduledThreadPool : 允许的创建线程数量为 Integer.MAX_VALUE，可能会创建大量的线程，从而导致 OOM。
 * <p>
 * ThreadPoolExecutor已经是Executor的具体实现
 *
 * @Author: jsy
 * @Date: 2021/5/23 14:35
 */
public class MainThread {

    // ArrayBlockingQueue;
    // LinkedBlockingQueue;
    // SynchronousQueue;
    // PriorityBlockingQueue
    private static ThreadPoolExecutor executor = new ThreadPoolExecutor(100,
            200, 100, TimeUnit.DAYS, new LinkedBlockingQueue<>(1000));

    // 定时调度线程池
    private static ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public static void main(String[] args) throws Exception {
        executor.execute(new Task1("参数"));
        scheduledExecutorService.scheduleWithFixedDelay(new Task1(), 10, 20, TimeUnit.SECONDS);
        // priorityQueueTest();
    }

    /**
     * priorityBlockingQueue是一个无界队列，它没有限制，在内存允许的情况下可以无限添加元素；它又是具有优先级的队列，是通过构造函数传入的对象来判断，
     * 传入的对象必须实现comparable接口。
     * @throws Exception
     */
    private static void priorityQueueTest() throws Exception {

        PriorityBlockingQueue<Person> pbq = new PriorityBlockingQueue<>();
        pbq.add(new Person(3, "person3"));
        System.err.println("容器为：" + pbq);
        pbq.add(new Person(2, "person2"));
        System.err.println("容器为：" + pbq);
        pbq.add(new Person(1, "person1"));
        System.err.println("容器为：" + pbq);
        pbq.add(new Person(4, "person4"));
        System.err.println("容器为：" + pbq);
        System.err.println("分割线----------------------------------------------------------------");

        System.err.println("获取元素 " + pbq.take().getId());
        System.err.println("容器为：" + pbq);
        System.err.println("分割线----------------------------------------------------------------");

        System.err.println("获取元素 " + pbq.take().getId());
        System.err.println("容器为：" + pbq);
        System.err.println("分割线----------------------------------------------------------------");

        System.err.println("获取元素 " + pbq.take().getId());
        System.err.println("容器为：" + pbq);
        System.err.println("分割线----------------------------------------------------------------");

        System.err.println("获取元素 " + pbq.take().getId());
        System.err.println("容器为：" + pbq);
        System.err.println("分割线----------------------------------------------------------------");
    }
}
