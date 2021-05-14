package com.aliyun.hitsdb.client.consumer;

import com.aliyun.hitsdb.client.Config;
import com.aliyun.hitsdb.client.TSDB;
import com.aliyun.hitsdb.client.callback.*;
import com.aliyun.hitsdb.client.http.HttpAPI;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.queue.DataQueue;
import com.aliyun.hitsdb.client.util.guava.RateLimiter;
import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.request.PointsCollection;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.aliyun.hitsdb.client.http.HttpClient.wrapDatabaseRequestParam;

public class PointsCollectionPutRunnable extends AbstractBatchPutRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(PointsCollectionPutRunnable.class);

    public PointsCollectionPutRunnable(TSDB tsdb, DataQueue dataQueue, HttpClient httpclient, CountDownLatch countDownLatch, Config config, RateLimiter rateLimiter) {
        super(tsdb, dataQueue, httpclient, countDownLatch, config, rateLimiter);
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        boolean readyClose = false;
        int waitTimeLimit = batchPutTimeLimit / 3;

        while (true) {
            if (readyClose) {
                break;
            }

            long t0 = System.currentTimeMillis();
            PointsCollection points;
            try {
                points = dataQueue.receivePoints(waitTimeLimit);
                if ((points == null) && (!readyClose)) {
                    continue;
                }
            } catch (InterruptedException itex) {
                readyClose = true;
                LOGGER.info("The thread {} is interrupted. cause {}", Thread.currentThread().getName(), itex.getMessage());
                break;
            }

            // acquire the permit before actually sending the post
            if (this.rateLimiter != null) {
                this.rateLimiter.acquire();
            }

            // serialization
            String strJson = points.toJSON();

            // 发送
            sendHttpRequest(points, strJson);
        }

        if (readyClose) {
            this.countDownLatch.countDown();
        }
    }

    private void sendHttpRequest(final PointsCollection points, String strJson) {
        if (points.isEmpty()) {
            LOGGER.warn("PointsCollection is empty, nothing to post");
            return;
        }

        String address = getAddressAndSemaphoreAcquire();

        /**
         * every time ready to send the write request,
         * it is necessary to get the current database in real time
         */
        String database = this.paramsMap.get("db");
        Map<String, String> queryParamsMap;
        if (database != null) {
            queryParamsMap = wrapDatabaseRequestParam(database);
        } else {
            queryParamsMap = new HashMap<String, String>();
        }
        if (points.getSimplePointBatchCallbak() != null) {
            AbstractBatchPutCallback scallback = points.getSimplePointBatchCallbak();


            if (scallback != null) {
                if (scallback instanceof BatchPutCallback) {
                } else if (scallback instanceof BatchPutSummaryCallback) {
                    queryParamsMap.put("summary", "true");
                } else if (scallback instanceof BatchPutDetailsCallback) {
                    queryParamsMap.put("details", "true");
                }
            }

            List<Point> slist = points.asSingleFieldPoints();
            FutureCallback<HttpResponse> postHttpCallback = this.httpResponseCallbackFactory
                    .createBatchPutDataCallback(
                            address,
                            scallback,
                            slist,
                            config,
                    config.getBatchPutRetryCount());

            try {
                tsdbHttpClient.postToAddress(address, HttpAPI.PUT, strJson, queryParamsMap, postHttpCallback);
            } catch (Exception ex) {
                this.semaphoreManager.release(address);
                scallback.failed(address, slist, ex);
            }

        } else {
            AbstractMultiFieldBatchPutCallback mcallback = points.getMultiFieldBatchPutCallback();

            if (mcallback != null) {
                if (mcallback instanceof MultiFieldBatchPutCallback) {
                } else if (mcallback instanceof MultiFieldBatchPutSummaryCallback) {
                    queryParamsMap.put("summary", "true");
                } else if (mcallback instanceof MultiFieldBatchPutDetailsCallback) {
                    queryParamsMap.put("details", "true");
                }


                List<MultiFieldPoint> mlist = points.asMultiFieldPoints();
                FutureCallback<HttpResponse> postHttpCallback = this.httpResponseCallbackFactory
                        .createMultiFieldBatchPutDataCallback(
                                address,
                                mcallback,
                                mlist,
                                config,
                                config.getBatchPutRetryCount());

                try {
                    tsdbHttpClient.postToAddress(address, HttpAPI.MPUT, strJson, queryParamsMap, postHttpCallback);
                } catch (Exception ex) {
                    this.semaphoreManager.release(address);
                    mcallback.failed(address, mlist, ex);
                }
            } else {
                LOGGER.warn("No batch callback at all");
            }
        }

    }
}
