package com.aliyun.hitsdb.client.callback.http;

import java.net.SocketTimeoutException;
import java.util.List;

import com.aliyun.hitsdb.client.Config;
import com.aliyun.hitsdb.client.HAPolicy;
import com.aliyun.hitsdb.client.callback.BatchPutIgnoreErrorsCallback;
import com.aliyun.hitsdb.client.value.request.AbstractPoint;
import com.aliyun.hitsdb.client.value.response.batch.IgnoreErrorsResult;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.callback.AbstractBatchPutCallback;
import com.aliyun.hitsdb.client.callback.BatchPutCallback;
import com.aliyun.hitsdb.client.callback.BatchPutDetailsCallback;
import com.aliyun.hitsdb.client.callback.BatchPutSummaryCallback;
import com.aliyun.hitsdb.client.exception.http.HttpClientConnectionRefusedException;
import com.aliyun.hitsdb.client.exception.http.HttpClientSocketTimeoutException;
import com.aliyun.hitsdb.client.exception.http.HttpServerErrorException;
import com.aliyun.hitsdb.client.exception.http.HttpServerNotSupportException;
import com.aliyun.hitsdb.client.exception.http.HttpUnknowStatusException;
import com.aliyun.hitsdb.client.http.HttpAPI;
import com.aliyun.hitsdb.client.http.HttpAddressManager;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.http.response.HttpStatus;
import com.aliyun.hitsdb.client.http.response.ResultResponse;
import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.response.batch.DetailsResult;
import com.aliyun.hitsdb.client.value.response.batch.SummaryResult;

public class BatchPutHttpResponseCallback extends AbstractPutHttpResponseCallback {
    private final AbstractBatchPutCallback<?> batchPutCallback;
    final List<Point> pointList;

    public BatchPutHttpResponseCallback(String address, HttpClient httpclient, AbstractBatchPutCallback<?> batchPutCallback,
                                        List<Point> pointList, Config config, int batchPutRetryTimes, HAPolicy.WriteContext writeContext) {
        super(address, httpclient, config, batchPutRetryTimes, writeContext);
        this.batchPutCallback = batchPutCallback;
        this.pointList = pointList;
    }

    public AbstractBatchPutCallback<?> getLogicalBatchPutCallback() {
        return batchPutCallback;
    }

    @Override
    public void completed(HttpResponse httpResponse) {
        HttpClient httpClient = writeContext == null ? this.hitsdbHttpClient : writeContext.getClient();
        try {
            // 处理响应
            if (httpResponse.getStatusLine().getStatusCode() == org.apache.http.HttpStatus.SC_TEMPORARY_REDIRECT) {
                httpClient.setSslEnable(true);
                if (errorRetry()) {
                    return;
                }
            }
            ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.compress);
            HttpStatus httpStatus = resultResponse.getHttpStatus();
            switch (httpStatus) {
                case ServerSuccess:
                case ServerSuccessNoContent:
                    if (writeContext != null) {
                        writeContext.success();
                    }
                    if (batchPutCallback == null) {
                        return;
                    }

                    if (batchPutCallback instanceof BatchPutCallback) {
                        ((BatchPutCallback) batchPutCallback).response(this.address, pointList, new Result());
                        return;
                    } else if (batchPutCallback instanceof BatchPutSummaryCallback) {
                        SummaryResult summaryResult = null;
                        if (!httpStatus.equals(HttpStatus.ServerSuccessNoContent)) {
                            String content = resultResponse.getContent();
                            summaryResult = JSON.parseObject(content, SummaryResult.class);
                        }
                        ((BatchPutSummaryCallback) batchPutCallback).response(this.address, pointList, summaryResult);
                        return;
                    } else if (batchPutCallback instanceof BatchPutDetailsCallback) {
                        DetailsResult detailsResult = null;
                        if (!httpStatus.equals(HttpStatus.ServerSuccessNoContent)) {
                            String content = resultResponse.getContent();
                            detailsResult = JSON.parseObject(content, DetailsResult.class);
                        }
                        ((BatchPutDetailsCallback) batchPutCallback).response(this.address, pointList, detailsResult);
                        return;
                    } else if (batchPutCallback instanceof BatchPutIgnoreErrorsCallback) {
                        IgnoreErrorsResult ignoreErrorsResult = null;
                        if (!httpStatus.equals(HttpStatus.ServerSuccessNoContent)) {
                            String content = resultResponse.getContent();
                            ignoreErrorsResult = JSON.parseObject(content, IgnoreErrorsResult.class);
                        }
                        ((BatchPutIgnoreErrorsCallback) batchPutCallback).response(this.address, pointList, ignoreErrorsResult);
                        return;
                    }
                case ServerNotSupport: {
                    // 服务器返回4xx错误
                    if (writeContext != null) {
                        writeContext.success();
                    }
                    HttpServerNotSupportException ex = new HttpServerNotSupportException(resultResponse);
                    this.failedWithResponse(ex);
                    return;
                }
                case ServerError: {
                    // 服务器返回5xx错误
                    if (this.batchPutRetryTimes == 0 && this.writeContext == null) {
                        HttpServerErrorException ex = new HttpServerErrorException(resultResponse);
                        this.failedWithResponse(ex);
                    } else {
                        if (!errorRetry()) {
                            LOGGER.warn("batch put retry limit {} reached", this.batchPutRetryTimes);
                            HttpServerErrorException ex = new HttpServerErrorException(resultResponse);
                            this.failedWithResponse(ex);
                        }
                    }

                    return;
                }
                default: {
                    HttpUnknowStatusException ex = new HttpUnknowStatusException(resultResponse);
                    this.failedWithResponse(ex);
                }
            }
        } finally {
            // 正常释放Semaphor
            httpClient.getSemaphoreManager().release(address);
        }
    }

    /**
     * 有响应的异常处理。
     *
     * @param ex
     */
    private void failedWithResponse(Exception ex) {
        if (batchPutCallback == null) { // 无回调逻辑，则失败打印日志。
            LOGGER.error("No callback logic exception. address:" + this.address, ex);
        } else {
            batchPutCallback.failed(this.address, pointList, ex);
        }
    }

    /**
     * execute the retry logic when writing failed
     * @return true if retry actually executed; false if retry limit reached
     */
    boolean errorRetry() {
        StringBuffer newAddressBuff = new StringBuffer();
        int retryTimes = this.batchPutRetryTimes;
        HttpClient httpClient = getHttpClient(newAddressBuff);
        if (httpClient == null) {
            return false;
        }
        String newAddress = newAddressBuff.toString();

        // retry!
        LOGGER.warn("retry put data!");
        HttpResponseCallbackFactory httpResponseCallbackFactory = httpClient.getHttpResponseCallbackFactory();

        FutureCallback<HttpResponse> retryCallback;
        if (batchPutCallback != null) {
            retryCallback = httpResponseCallbackFactory.createBatchPutDataCallback(newAddress,
                    this.batchPutCallback, this.pointList, this.config, retryTimes, writeContext);
        } else {
            retryCallback = httpResponseCallbackFactory.createNoLogicBatchPutHttpFutureCallback(newAddress,
                    this.pointList, this.config, retryTimes, this.writeContext);
        }

        String jsonString = JSON.toJSONString(pointList);
        httpClient.post(HttpAPI.PUT, jsonString, retryCallback);
        return true;
    }


    @Override
    public void failed(Exception ex) {
        HttpClient httpClient = writeContext == null ? this.hitsdbHttpClient : writeContext.getClient();
        try {
            // 异常重试
            if (ex instanceof SocketTimeoutException) {
                if (this.batchPutRetryTimes == 0 && this.writeContext == null) {
                    ex = new HttpClientSocketTimeoutException(ex);
                } else {
                    if (errorRetry()) {
                        return;
                    }
                }
            } else if (ex instanceof java.net.ConnectException) {
                if (this.batchPutRetryTimes == 0 && this.writeContext == null) {
                    ex = new HttpClientConnectionRefusedException(this.address, ex);
                } else {
                    if (errorRetry()) {
                        return;
                    }
                }
            }

            // 处理完毕，向逻辑层传递异常并处理。
            if (batchPutCallback == null) {
                LOGGER.error("No callback logic exception.", ex);
            } else {
                batchPutCallback.failed(this.address, pointList, ex);
            }
        } finally {
            // 重试后释放semaphore许可
            httpClient.getSemaphoreManager().release(address);
        }
    }
}