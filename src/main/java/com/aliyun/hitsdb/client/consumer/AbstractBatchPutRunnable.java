package com.aliyun.hitsdb.client.consumer;

import com.aliyun.hitsdb.client.Config;
import com.aliyun.hitsdb.client.callback.http.HttpResponseCallbackFactory;
import com.aliyun.hitsdb.client.http.HttpAddressManager;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.http.semaphore.SemaphoreManager;
import com.aliyun.hitsdb.client.queue.DataQueue;
import com.aliyun.hitsdb.client.util.guava.RateLimiter;

import java.util.concurrent.CountDownLatch;

public abstract class AbstractBatchPutRunnable {
    /**
     * 缓冲队列
     */
    protected final DataQueue dataQueue;
    /**
     * Http客户端
     */
    protected final HttpClient tsdbHttpClient;
    protected final HttpClient secondaryClient;
    /**
     * 消费者队列控制器。
     * 在优雅关闭中，若消费者队列尚未结束，则CountDownLatch用于阻塞close()方法。
     */
    protected final CountDownLatch countDownLatch;
    protected final Config config;
    protected final SemaphoreManager semaphoreManager;
    protected final HttpAddressManager httpAddressManager;
    /**
     * 回调包装与构造工厂
     */
    protected final HttpResponseCallbackFactory httpResponseCallbackFactory;
    /**
     * 每批次数据点个数
     */
    protected int batchSize;
    /**
     * 批次提交间隔，单位：毫秒
     */
    protected int batchPutTimeLimit;
    protected final RateLimiter rateLimiter;

    public AbstractBatchPutRunnable(DataQueue dataQueue, HttpClient httpclient, HttpClient secondaryClient, CountDownLatch countDownLatch, Config config, RateLimiter rateLimiter) {
        this.dataQueue = dataQueue;
        this.tsdbHttpClient = httpclient;
        this.secondaryClient = secondaryClient;
        this.countDownLatch = countDownLatch;
        this.batchSize = config.getBatchPutSize();
        this.batchPutTimeLimit = config.getBatchPutTimeLimit();
        this.config = config;
        this.semaphoreManager = tsdbHttpClient.getSemaphoreManager();
        this.httpAddressManager = tsdbHttpClient.getHttpAddressManager();
        this.rateLimiter = rateLimiter;
        this.httpResponseCallbackFactory = tsdbHttpClient.getHttpResponseCallbackFactory();
    }
}
