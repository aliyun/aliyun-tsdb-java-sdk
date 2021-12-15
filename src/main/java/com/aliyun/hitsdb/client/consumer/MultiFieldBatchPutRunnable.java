package com.aliyun.hitsdb.client.consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.aliyun.hitsdb.client.Config;
import com.aliyun.hitsdb.client.TSDB;
import com.aliyun.hitsdb.client.callback.*;
import com.aliyun.hitsdb.client.http.HAHttpClient;
import com.aliyun.hitsdb.client.http.HttpAPI;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.queue.DataQueue;
import com.aliyun.hitsdb.client.value.request.UniqueUtil;
import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;
import com.aliyun.hitsdb.client.util.guava.RateLimiter;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.aliyun.hitsdb.client.http.HttpClient.wrapDatabaseRequestParam;

public class MultiFieldBatchPutRunnable extends AbstractBatchPutRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiFieldBatchPutRunnable.class);

    /**
     * 批量提交回调
     */
    private final AbstractMultiFieldBatchPutCallback<?> multiFieldBatchPutCallback;

    public MultiFieldBatchPutRunnable(TSDB tsdb, DataQueue dataQueue, HAHttpClient httpclient, Config config, CountDownLatch countDownLatch, RateLimiter rateLimiter) {
        super(tsdb, dataQueue, httpclient, countDownLatch, config, rateLimiter);
        this.multiFieldBatchPutCallback = config.getMultiFieldBatchPutCallback();

        if (this.multiFieldBatchPutCallback != null) {
            if (multiFieldBatchPutCallback instanceof MultiFieldBatchPutCallback) {
            } else if (multiFieldBatchPutCallback instanceof MultiFieldBatchPutSummaryCallback) {
                paramsMap.put("summary", "true");
            } else if (multiFieldBatchPutCallback instanceof MultiFieldBatchPutDetailsCallback) {
                paramsMap.put("details", "true");
            } else if (multiFieldBatchPutCallback instanceof MultiFieldBatchPutIgnoreErrorsCallback) {
                paramsMap.put("ignoreErrors", "true");
            }
        }
    }

    @Override
    public void run() {

        MultiFieldPoint waitPoint = null;
        boolean readyClose = false;
        int waitTimeLimit = batchPutTimeLimit / 3;

        while (true) {
            if (readyClose && waitPoint == null) {
                break;
            }

            long t0 = System.currentTimeMillis();
            List<MultiFieldPoint> pointList = new ArrayList<MultiFieldPoint>(batchSize);
            if (waitPoint != null) {
                pointList.add(waitPoint);
                waitPoint = null;
            }

            for (int i = pointList.size(); i < batchSize; i++) {
                try {
                    MultiFieldPoint point = dataQueue.receiveMultiFieldPoint(waitTimeLimit);
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
                    MultiFieldPoint newPoint = dataQueue.receiveMultiFieldPoint();
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
            UniqueUtil.uniqueMultiFieldPoints(pointList, config.isDeduplicationEnable());

            // 序列化
            String strJson = serialize(pointList);

            // 发送
            sendHttpRequest(pointList, strJson, paramsMap);
        }

        if (readyClose) {
            this.countDownLatch.countDown();
        }
    }


    private void sendHttpRequest(List<MultiFieldPoint> pointList, String strJson, Map<String, String> paramsMap) {
        HttpClient httpClient = tsdbHttpClient.getWriteClient();
        String address = httpClient.getAddressAndSemaphoreAcquire();
        if (this.multiFieldBatchPutCallback != null) {
            FutureCallback<HttpResponse> postHttpCallback = this.httpResponseCallbackFactory
                    .createMultiFieldBatchPutDataCallback(
                            address,
                            this.multiFieldBatchPutCallback,
                            pointList,
                            config,
                            config.getBatchPutRetryCount());

            try {
                httpClient.postToAddress(address, HttpAPI.MPUT, strJson, paramsMap, postHttpCallback);
            } catch (Exception ex) {
                httpClient.getSemaphoreManager().release(address);
                this.multiFieldBatchPutCallback.failed(address, pointList, ex);
            }
        } else {
            FutureCallback<HttpResponse> noLogicBatchPutHttpFutureCallback = this.httpResponseCallbackFactory
                    .createMultiFieldNoLogicBatchPutHttpFutureCallback(
                            address,
                            pointList,
                            config,
                            config.getBatchPutRetryCount()
                    );
            try {
                httpClient.postToAddress(address, HttpAPI.MPUT, strJson, noLogicBatchPutHttpFutureCallback);
            } catch (Exception ex) {
                httpClient.getSemaphoreManager().release(address);
                noLogicBatchPutHttpFutureCallback.failed(ex);
            }
        }
    }

    private String serialize(List<MultiFieldPoint> pointList) {
        return JSON.toJSONString(pointList, SerializerFeature.DisableCircularReferenceDetect);
    }

}