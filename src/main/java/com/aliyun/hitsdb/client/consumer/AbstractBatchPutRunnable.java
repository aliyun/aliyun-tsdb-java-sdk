package com.aliyun.hitsdb.client.consumer;

import com.aliyun.hitsdb.client.Config;
import com.aliyun.hitsdb.client.TSDB;
import com.aliyun.hitsdb.client.callback.http.HttpResponseCallbackFactory;
import com.aliyun.hitsdb.client.event.TSDBDatabaseChangedEvent;
import com.aliyun.hitsdb.client.event.TSDBDatabaseChangedListener;
import com.aliyun.hitsdb.client.http.HAHttpClient;
import com.aliyun.hitsdb.client.http.HttpAddressManager;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.http.semaphore.SemaphoreManager;
import com.aliyun.hitsdb.client.queue.DataQueue;
import com.aliyun.hitsdb.client.util.guava.RateLimiter;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.aliyun.hitsdb.client.http.HttpClient.wrapDatabaseRequestParam;
import static com.aliyun.hitsdb.client.http.HttpClient.updateDatabaseRequestParam;

public abstract class AbstractBatchPutRunnable {
    /**
     * 缓冲队列
     */
    protected final DataQueue dataQueue;
    /**
     * Http客户端
     */
    protected HAHttpClient tsdbHttpClient;

    /**
     * 消费者队列控制器。
     * 在优雅关闭中，若消费者队列尚未结束，则CountDownLatch用于阻塞close()方法。
     */
    protected final CountDownLatch countDownLatch;
    protected final Config config;
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

    /**
     * the map containing query parameters for building the write HTTP request
     * the possible query parameters might be: "db", "summary", "details" ...
     */
    protected Map<String, String> paramsMap;


    public AbstractBatchPutRunnable(TSDB tsdb ,DataQueue dataQueue, HAHttpClient httpclient, CountDownLatch countDownLatch, Config config, RateLimiter rateLimiter) {
        this.dataQueue = dataQueue;
        this.tsdbHttpClient = httpclient;
        this.countDownLatch = countDownLatch;
        this.batchSize = config.getBatchPutSize();
        this.batchPutTimeLimit = config.getBatchPutTimeLimit();
        this.config = config;
        this.rateLimiter = rateLimiter;
        this.httpResponseCallbackFactory = tsdbHttpClient.getHttpResponseCallbackFactory();
        this.paramsMap = wrapDatabaseRequestParam(tsdb.getCurrentDatabase());

        // register the database changed event listener
        tsdb.addDatabaseChangedListener(new TSDBDatabaseChangedListener() {
            @Override
            public void databaseChanged(TSDBDatabaseChangedEvent event) {
                /**
                 * every time the currently in use database changed,
                 * it is necessary to update the "db" query parameter in real time
                 */
                updateDatabaseRequestParam(paramsMap, event.getCurrentDatabase());
            }
        });
    }
}
