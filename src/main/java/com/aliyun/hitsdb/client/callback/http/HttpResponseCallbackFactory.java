package com.aliyun.hitsdb.client.callback.http;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.aliyun.hitsdb.client.Config;
import com.aliyun.hitsdb.client.HAPolicy;
import com.aliyun.hitsdb.client.callback.AbstractMultiFieldBatchPutCallback;
import com.aliyun.hitsdb.client.util.Objects;
import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;

import com.aliyun.hitsdb.client.callback.AbstractBatchPutCallback;
import com.aliyun.hitsdb.client.callback.QueryCallback;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.request.Query;

public class HttpResponseCallbackFactory {
    private final AtomicInteger unCompletedTaskNum;
    private final HttpClient hitsdbHttpclient;
    private final boolean httpCompress;

    public HttpResponseCallbackFactory(AtomicInteger unCompletedTaskNum, HttpClient httpclient, boolean httpCompress) {
        this.unCompletedTaskNum = unCompletedTaskNum;
        this.hitsdbHttpclient = httpclient;
        this.httpCompress = httpCompress;
    }

    public FutureCallback<HttpResponse> wrapUpBaseHttpFutureCallback(final FutureCallback<HttpResponse> futureCallback) {
        Objects.requireNonNull(futureCallback);
        return new BaseHttpFutureCallback(unCompletedTaskNum, futureCallback);
    }

    public FutureCallback<HttpResponse> createQueryCallback(final String address, final QueryCallback callback, final Query query) {
        FutureCallback<HttpResponse> httpCallback = new QueryHttpResponseCallback(address, query, callback, this.httpCompress);
        return httpCallback;
    }

    public FutureCallback<HttpResponse> createBatchPutDataCallback(
            final String address,
            final AbstractBatchPutCallback<?> batchPutCallback,
            final List<Point> pointList,
            final Config config,
            final int retryTimes,
            final HAPolicy.WriteContext writeContext) {
        FutureCallback<HttpResponse> httpCallback = new BatchPutHttpResponseCallback(
                address,
                hitsdbHttpclient,
                batchPutCallback,
                pointList,
                config,
                retryTimes,
                writeContext
        );
        return httpCallback;
    }


    public FutureCallback<HttpResponse> createNoLogicBatchPutHttpFutureCallback(
            final String address,
            final List<Point> pointList,
            final Config config,
            final int batchPutRetryTimes,
            final HAPolicy.WriteContext writeContext
    ) {
        FutureCallback<HttpResponse> httpCallback =
                new BatchPutHttpResponseCallback(
                        address,
                        hitsdbHttpclient,
                        null,
                        pointList,
                        config,
                        batchPutRetryTimes,
                        writeContext
                );
        return httpCallback;
    }



    public FutureCallback<HttpResponse> createMultiFieldBatchPutDataCallback(
            final String address,
            final AbstractMultiFieldBatchPutCallback<?> batchPutCallback,
            final List<MultiFieldPoint> pointList,
            final Config config,
            final int retryTimes,
            final HAPolicy.WriteContext writeContext) {
        FutureCallback<HttpResponse> httpCallback = new MultiFieldBatchPutHttpResponseCallback(
                address,
                hitsdbHttpclient,
                batchPutCallback,
                pointList,
                config,
                retryTimes,
                writeContext
        );
        return httpCallback;
    }


    public FutureCallback<HttpResponse> createMultiFieldNoLogicBatchPutHttpFutureCallback(
            final String address,
            final List<MultiFieldPoint> pointList,
            final Config config,
            final int batchPutRetryTimes,
            final HAPolicy.WriteContext writeContext
    ) {
        FutureCallback<HttpResponse> httpCallback =
                new MultiFieldBatchPutHttpResponseCallback(
                        address,
                        hitsdbHttpclient,
                        null,
                        pointList,
                        config,
                        batchPutRetryTimes,
                        writeContext
                );
        return httpCallback;
    }

}
