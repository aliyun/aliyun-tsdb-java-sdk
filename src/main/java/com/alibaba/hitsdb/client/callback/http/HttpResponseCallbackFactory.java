package com.alibaba.hitsdb.client.callback.http;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;

import com.alibaba.hitsdb.client.HiTSDBConfig;
import com.alibaba.hitsdb.client.callback.AbstractBatchPutCallback;
import com.alibaba.hitsdb.client.callback.QueryCallback;
import com.alibaba.hitsdb.client.http.HttpClient;
import com.alibaba.hitsdb.client.value.request.Point;
import com.alibaba.hitsdb.client.value.request.Query;

public class HttpResponseCallbackFactory {
    private final AtomicInteger unCompletedTaskNum;
    private final HttpClient hitsdbHttpclient;
    private final boolean httpCompress;

    public HttpResponseCallbackFactory(AtomicInteger unCompletedTaskNum,HttpClient httpclient,boolean httpCompress) {
        this.unCompletedTaskNum = unCompletedTaskNum;
        this.hitsdbHttpclient = httpclient;
        this.httpCompress = httpCompress;
    }

    public FutureCallback<HttpResponse> wrapUpBaseHttpFutureCallback(final FutureCallback<HttpResponse> futureCallback) {
        Objects.requireNonNull(futureCallback);
        return new BaseHttpFutrueCallback(unCompletedTaskNum, futureCallback);
    }

    public FutureCallback<HttpResponse> createQueryCallback(final String address, final QueryCallback callback, final Query query) {
        FutureCallback<HttpResponse> httpCallback = new QueryHttpResponseCallback(address, query, callback,this.httpCompress);
        return httpCallback;
    }

    public FutureCallback<HttpResponse> createBatchPutDataCallback(
    			final String address,
            final AbstractBatchPutCallback<?> batchPutCallback,
            final List<Point> pointList,
            final HiTSDBConfig config,
            final int batchPutRetryCount
    ) {
        FutureCallback<HttpResponse> httpCallback = new BatchPutHttpResponseCallback (
					address,
					hitsdbHttpclient,
					batchPutCallback,
					pointList,
					config,
					config.getBatchPutRetryCount()
                );
        return httpCallback;
    }
    
    
    public FutureCallback<HttpResponse> createNoLogicBatchPutHttpFutureCallback(
    			final String address,
            final List<Point> pointList,
            final HiTSDBConfig config,
            final int batchPutRetryTimes
    ) {
        FutureCallback<HttpResponse> httpCallback = 
                new BatchPutHttpResponseCallback (
					address,
					hitsdbHttpclient,
					null,
					pointList,
					config,
					batchPutRetryTimes
                );
        return httpCallback;
    }
    
}
