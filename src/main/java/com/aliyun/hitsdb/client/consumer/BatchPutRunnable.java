package com.aliyun.hitsdb.client.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import com.alibaba.fastjson.serializer.SerializerFeature;
import com.aliyun.hitsdb.client.Config;
import com.aliyun.hitsdb.client.HAPolicy;
import com.aliyun.hitsdb.client.callback.*;
import com.aliyun.hitsdb.client.value.request.UniqueUtil;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.http.HttpAPI;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.queue.DataQueue;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.util.guava.RateLimiter;

import static com.aliyun.hitsdb.client.callback.AbstractBatchPutCallback.getPutQueryParamMap;

public class BatchPutRunnable extends AbstractBatchPutRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchPutRunnable.class);

    /**
     * 批量提交回调
     */
    private final AbstractBatchPutCallback<?> batchPutCallback;


    public BatchPutRunnable(DataQueue dataQueue, HttpClient httpclient, HttpClient secondaryClient, Config config, CountDownLatch countDownLatch, RateLimiter rateLimiter) {
        super(dataQueue, httpclient, secondaryClient, countDownLatch, config, rateLimiter);
        this.batchPutCallback = config.getBatchPutCallback();
    }

    @Override
    public void run() {
        Map<String, String> paramsMap = getPutQueryParamMap(batchPutCallback);

        Point waitPoint = null;
        boolean readyClose = false;
        int waitTimeLimit = batchPutTimeLimit / 3;

        while (true) {
            if (readyClose && waitPoint == null) {
                break;
            }

            long t0 = System.currentTimeMillis();
            List<Point> pointList = new ArrayList<Point>(batchSize);
            if (waitPoint != null) {
                pointList.add(waitPoint);
                waitPoint = null;
            }

            for (int i = pointList.size(); i < batchSize; i++) {
                try {
                    Point point = dataQueue.receive(waitTimeLimit);
                    if (point != null) {
                        if (this.rateLimiter != null) {
                            this.rateLimiter.acquire();
                        }
                        pointList.add(point);
                    }
                    long t1 = System.currentTimeMillis();
                    if (t1 - t0 > batchPutTimeLimit) {
                        break;
                    }
                } catch (InterruptedException e) {
                    readyClose = true;
                    LOGGER.info("The thread {} is interrupted", Thread.currentThread().getName());
                    break;
                }
            }

            if (pointList.size() == 0 && !readyClose) {
                try {
                    Point newPoint = dataQueue.receive();
                    waitPoint = newPoint;
                    continue;
                } catch (InterruptedException e) {
                    readyClose = true;
                    LOGGER.info("The thread {} is interrupted", Thread.currentThread().getName());
                }
            }

            if (pointList.size() == 0) {
                continue;
            }

            //去重
            UniqueUtil.uniquePoints(pointList, config.isDeduplicationEnable());

            // 序列化
            String strJson = serialize(pointList);

            // 发送
            sendHttpRequest(pointList, strJson, paramsMap);
        }

        if (readyClose) {
            this.countDownLatch.countDown();
        }
    }


    //TODO: unify the implementation with TSDBClient#putAsync()
    private void sendHttpRequest(List<Point> pointList, String strJson, Map<String, String> paramsMap) {
        HttpClient httpClient;
        HAPolicy.WriteContext writeContext = null;
        if (config.getHAPolicy() != null && config.getHAPolicy().getWriteRetryTimes() > 0) {
            writeContext = config.getHAPolicy().creatWriteContext();
            httpClient = writeContext.getClient();
        } else {
            httpClient = this.tsdbHttpClient;
        }
        String address = httpClient.getAddressAndSemaphoreAcquire();
        if (this.batchPutCallback != null) {
            FutureCallback<HttpResponse> postHttpCallback = this.httpResponseCallbackFactory
                    .createBatchPutDataCallback(
                            address,
                            this.batchPutCallback,
                            pointList,
                            config,
                            config.getBatchPutRetryCount(),
                            writeContext);

            try {
                httpClient.postToAddress(address, HttpAPI.PUT, strJson, paramsMap, postHttpCallback);
            } catch (Exception ex) {
                this.semaphoreManager.release(address);
                this.batchPutCallback.failed(address, pointList, ex);
            }
        } else {
            FutureCallback<HttpResponse> noLogicBatchPutHttpFutureCallback = this.httpResponseCallbackFactory
                    .createNoLogicBatchPutHttpFutureCallback(
                            address,
                            pointList,
                            config,
                            config.getBatchPutRetryCount(),
                            writeContext
                    );
            try {
                httpClient.postToAddress(address, HttpAPI.PUT, strJson, noLogicBatchPutHttpFutureCallback);
            } catch (Exception ex) {
                this.semaphoreManager.release(address);
                noLogicBatchPutHttpFutureCallback.failed(ex);
            }
        }
    }

    private String serialize(List<Point> pointList) {
        return JSON.toJSONString(pointList, SerializerFeature.DisableCircularReferenceDetect);
    }

}