package com.alibaba.hitsdb.client.performance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.hitsdb.client.value.request.Point;

public class TestBlockingQueue {
    static final int P_NUM = 10;
    static final int C_NUM = 4;
    static final int SIZE = 3000000;
    static final AtomicLong num = new AtomicLong();
    static final BlockingQueue<Point> QUEUE = new ArrayBlockingQueue<Point>(20000);
    static final AtomicLong t0 = new AtomicLong();
    static final AtomicLong t1 = new AtomicLong();

    static final CountDownLatch countDownLatch = new CountDownLatch(P_NUM + C_NUM);
    static final int BatchPutSize = 100;
    
    static class RunnableXiaofei implements Runnable {

        @Override
        public void run() {
            Thread.currentThread().setName("消费线程-Thread");
            boolean goOut = false;
            for (;;) {
                List<Point> points = new ArrayList<Point>(BatchPutSize);
                for (int i = 0; i < BatchPutSize; i++) {
                    try {
                        Point ob = QUEUE.poll(200, TimeUnit.MILLISECONDS);
                        if (ob == null) {
                            goOut = true;
                            break;
                        }

                        points.add(ob);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                // JSON.toJSONString(points);

                if (goOut) {
                    break;
                }
            }

            countDownLatch.countDown();
        }

    }

    @Before
    public void init() throws IOException {
        System.out.println("按下任意键，开始运行...");
        while (true) {
            int read = System.in.read();
            if (read != 0) {
                break;
            }
        }

        ExecutorService threadPool = Executors.newFixedThreadPool(C_NUM);
        RunnableXiaofei runnableXiaofei = new RunnableXiaofei();

        for (int i = 0; i < C_NUM; i++) {
            threadPool.submit(runnableXiaofei);
        }

    }

    @After
    public void end() throws IOException {
        // 优雅关闭
        t1.compareAndSet(0, System.currentTimeMillis());

        double dt = t1.get() - t0.get();
        System.out.println("处理：" + num);
        System.out.println("时间：" + (dt));
        System.out.println("消耗速率" + SIZE * P_NUM / dt + "K/s");
        System.out.println("结束");
    }

    @Test
    public void run() throws InterruptedException {
        Thread[] p_threads = new Thread[P_NUM];
        for (int x = 0; x < p_threads.length; x++) {
            final int index = x;
            p_threads[index] = new Thread(new Runnable() {
                final Random random = new Random();

                @Override
                public void run() {
                    t0.compareAndSet(0, System.currentTimeMillis());
                    for (int i = 0; i < SIZE; i++) {
                        double nextDouble = random.nextDouble();
                        Point point = createPoint(index, nextDouble);
                        try {
                            QUEUE.put(point);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    // 该线程发送完
                    countDownLatch.countDown();
                    System.out.println("线程写入结束！");
                }
            });
        }

        // start
        for (int x = 0; x < p_threads.length; x++) {
            p_threads[x].start();
        }

        // 发送线程发送完毕
        countDownLatch.await();
        System.out.println("主线程将要结束，尝试优雅关闭");
    }

    public Point createPoint(int tag, double value) {
        int t = (int) (System.currentTimeMillis() / 1000);
        return Point.metric("test-performance").tag("tag", String.valueOf(tag)).value(t, value).build();
    }

}
