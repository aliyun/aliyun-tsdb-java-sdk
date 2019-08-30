package com.aliyun.hitsdb.client.consumer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.aliyun.hitsdb.client.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.queue.DataQueue;
import com.aliyun.hitsdb.client.util.guava.RateLimiter;

public class DefaultBatchPutConsumer implements Consumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultBatchPutConsumer.class);
    private DataQueue dataQueue;
    private ExecutorService threadPool;
    private ExecutorService multiFieldThreadPool;
    private int batchPutConsumerThreadCount;
    private int multiFieldBatchPutConsumerThreadCount;
    private HttpClient httpclient;
    private Config config;
    private RateLimiter rateLimiter;
    private CountDownLatch countDownLatch;

    public DefaultBatchPutConsumer(DataQueue buffer, HttpClient httpclient, RateLimiter rateLimiter, Config config) {
        this.dataQueue = buffer;
        this.httpclient = httpclient;
        this.config = config;
        this.batchPutConsumerThreadCount = config.getBatchPutConsumerThreadCount();
        this.multiFieldBatchPutConsumerThreadCount = config.getMultiFieldBatchPutConsumerThreadCount();
        this.rateLimiter = rateLimiter;
        if (batchPutConsumerThreadCount > 0) {
            this.threadPool = Executors.newFixedThreadPool(batchPutConsumerThreadCount, new BatchPutThreadFactory("batch-put-thread"));
        }
        if (multiFieldBatchPutConsumerThreadCount > 0) {
            this.multiFieldThreadPool = Executors.newFixedThreadPool(multiFieldBatchPutConsumerThreadCount, new BatchPutThreadFactory("multi-field-batch-put-thread"));
        }
        this.countDownLatch = new CountDownLatch(config.getBatchPutConsumerThreadCount() + config.getMultiFieldBatchPutConsumerThreadCount());
    }

    public void start() {
        for (int i = 0; i < batchPutConsumerThreadCount; i++) {
            threadPool.submit(new BatchPutRunnable(this.dataQueue, this.httpclient, this.config, this.countDownLatch, this.rateLimiter));
        }

        for (int i = 0; i < multiFieldBatchPutConsumerThreadCount; i++) {
            multiFieldThreadPool.submit(new MultiFieldBatchPutRunnable(this.dataQueue, this.httpclient, this.config, this.countDownLatch, this.rateLimiter));
        }
    }

    @Override
    public void stop() {
        this.stop(false);
    }

    @Override
    public void stop(boolean force) {
        if (force) {
            // 强制退出不等待，截断消费者线程。
            if (threadPool != null) {
                threadPool.shutdownNow();
            }

            if (multiFieldThreadPool != null) {
                multiFieldThreadPool.shutdownNow();
            }

        } else {
            if (threadPool != null) {
                // 截断消费者线程。
                while (!threadPool.isShutdown() || !threadPool.isTerminated()) {
                    threadPool.shutdownNow();
                }
            }

            if (multiFieldThreadPool != null) {
                // 截断消费者线程。
                while (!multiFieldThreadPool.isShutdown() || !multiFieldThreadPool.isTerminated()) {
                    multiFieldThreadPool.shutdownNow();
                }
            }

            // 等待所有消费者线程结束。
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                LOGGER.error("An error occurred waiting for the consumer thread to close", e);
            }
        }
        if (dataQueue != null) {
            dataQueue = null;
        }
    }
}
