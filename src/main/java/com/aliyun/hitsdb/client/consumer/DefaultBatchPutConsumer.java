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
    private ExecutorService pointsThreadPool;
    private int batchPutConsumerThreadCount;
    private int multiFieldBatchPutConsumerThreadCount;
    private int pointsBatchPutConsumerThreadCount;
    private HttpClient httpclient;
    private HttpClient secondaryClient;
    private Config config;
    private RateLimiter rateLimiter;
    private CountDownLatch countDownLatch;

    public DefaultBatchPutConsumer(DataQueue buffer, HttpClient httpclient, HttpClient secondaryClient, RateLimiter rateLimiter, Config config) {
        this.dataQueue = buffer;
        this.httpclient = httpclient;
        this.secondaryClient = secondaryClient;
        this.config = config;
        this.batchPutConsumerThreadCount = config.getBatchPutConsumerThreadCount();
        this.multiFieldBatchPutConsumerThreadCount = config.getMultiFieldBatchPutConsumerThreadCount();
        this.pointsBatchPutConsumerThreadCount = Math.max(this.batchPutConsumerThreadCount, this.multiFieldBatchPutConsumerThreadCount);

        this.rateLimiter = rateLimiter;
        if (batchPutConsumerThreadCount > 0) {
            this.threadPool = Executors.newFixedThreadPool(batchPutConsumerThreadCount, new BatchPutThreadFactory("batch-put-thread"));
        }
        if (multiFieldBatchPutConsumerThreadCount > 0) {
            this.multiFieldThreadPool = Executors.newFixedThreadPool(multiFieldBatchPutConsumerThreadCount, new BatchPutThreadFactory("multi-field-batch-put-thread"));
        }

        if (this.pointsBatchPutConsumerThreadCount > 0) {
            this.pointsThreadPool = Executors.newFixedThreadPool(pointsBatchPutConsumerThreadCount, new BatchPutThreadFactory("points-batch-put-thread"));
        }

        this.countDownLatch = new CountDownLatch(config.getBatchPutConsumerThreadCount() + config.getMultiFieldBatchPutConsumerThreadCount() + this.pointsBatchPutConsumerThreadCount);
    }

    public void start() {
        for (int i = 0; i < batchPutConsumerThreadCount; i++) {
            threadPool.submit(new BatchPutRunnable(this.dataQueue, this.httpclient, this.secondaryClient, this.config, this.countDownLatch, this.rateLimiter));
        }

        for (int i = 0; i < multiFieldBatchPutConsumerThreadCount; i++) {
            multiFieldThreadPool.submit(new MultiFieldBatchPutRunnable(this.dataQueue, this.httpclient, this.secondaryClient, this.config, this.countDownLatch, this.rateLimiter));
        }

        for (int i = 0; i < pointsBatchPutConsumerThreadCount; i++) {
            pointsThreadPool.submit(new PointsCollectionPutRunnable(this.dataQueue, this.httpclient, this.secondaryClient, this.countDownLatch, this.config, this.rateLimiter));
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

            if (pointsThreadPool != null) {
                pointsThreadPool.shutdownNow();
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

            if (pointsThreadPool != null) {
                while (!pointsThreadPool.isShutdown() || !pointsThreadPool.isTerminated()) {
                    pointsThreadPool.shutdownNow();
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
