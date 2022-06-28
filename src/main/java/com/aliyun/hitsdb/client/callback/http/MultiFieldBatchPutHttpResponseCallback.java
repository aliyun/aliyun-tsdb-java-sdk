package com.aliyun.hitsdb.client.callback.http;

import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.Config;
import com.aliyun.hitsdb.client.HAPolicy;
import com.aliyun.hitsdb.client.callback.AbstractMultiFieldBatchPutCallback;
import com.aliyun.hitsdb.client.callback.MultiFieldBatchPutCallback;
import com.aliyun.hitsdb.client.callback.MultiFieldBatchPutDetailsCallback;
import com.aliyun.hitsdb.client.callback.MultiFieldBatchPutIgnoreErrorsCallback;
import com.aliyun.hitsdb.client.callback.MultiFieldBatchPutSummaryCallback;
import com.aliyun.hitsdb.client.exception.http.*;
import com.aliyun.hitsdb.client.http.HttpAPI;
import com.aliyun.hitsdb.client.http.HttpAddressManager;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.http.response.HttpStatus;
import com.aliyun.hitsdb.client.http.response.ResultResponse;
import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;
import com.aliyun.hitsdb.client.value.response.batch.MultiFieldDetailsResult;
import com.aliyun.hitsdb.client.value.response.batch.MultiFieldIgnoreErrorsResult;
import com.aliyun.hitsdb.client.value.response.batch.SummaryResult;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Map;

public class MultiFieldBatchPutHttpResponseCallback extends AbstractPutHttpResponseCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiFieldBatchPutHttpResponseCallback.class);

    private final AbstractMultiFieldBatchPutCallback<?> multiFieldBatchPutCallback;
    private final List<MultiFieldPoint> pointList;

    public MultiFieldBatchPutHttpResponseCallback(String address, HttpClient httpclient, AbstractMultiFieldBatchPutCallback<?> multiFieldBatchPutCallback,
                                                  List<MultiFieldPoint> pointList, Config config, int batchPutRetryTimes, HAPolicy.WriteContext writeContext) {
        super(address, httpclient, config, batchPutRetryTimes, writeContext);
        this.multiFieldBatchPutCallback = multiFieldBatchPutCallback;
        this.pointList = pointList;
    }

    public AbstractMultiFieldBatchPutCallback<?> getLogicalBatchPutCallback() {
        return multiFieldBatchPutCallback;
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
                    if (multiFieldBatchPutCallback == null) {
                        return;
                    }

                    if (multiFieldBatchPutCallback instanceof MultiFieldBatchPutCallback) {
                        ((MultiFieldBatchPutCallback) multiFieldBatchPutCallback).response(this.address, pointList, new Result());
                        return;
                    } else if (multiFieldBatchPutCallback instanceof MultiFieldBatchPutSummaryCallback) {
                        SummaryResult summaryResult = null;
                        if (!httpStatus.equals(HttpStatus.ServerSuccessNoContent)) {
                            String content = resultResponse.getContent();
                            summaryResult = JSON.parseObject(content, SummaryResult.class);
                        }
                        ((MultiFieldBatchPutSummaryCallback) multiFieldBatchPutCallback).response(this.address, pointList, summaryResult);
                        return;
                    } else if (multiFieldBatchPutCallback instanceof MultiFieldBatchPutDetailsCallback) {
                        MultiFieldDetailsResult detailsResult = null;
                        if (!httpStatus.equals(HttpStatus.ServerSuccessNoContent)) {
                            String content = resultResponse.getContent();
                            detailsResult = JSON.parseObject(content, MultiFieldDetailsResult.class);
                        }
                        ((MultiFieldBatchPutDetailsCallback) multiFieldBatchPutCallback).response(this.address, pointList, detailsResult);
                        return;
                    } else if (multiFieldBatchPutCallback instanceof MultiFieldBatchPutIgnoreErrorsCallback) {
                        MultiFieldIgnoreErrorsResult ignoreErrorsResult = null;
                        if (!httpStatus.equals(HttpStatus.ServerSuccessNoContent)) {
                            String content = resultResponse.getContent();
                            ignoreErrorsResult = JSON.parseObject(content, MultiFieldIgnoreErrorsResult.class);
                        }
                        ((MultiFieldBatchPutIgnoreErrorsCallback) multiFieldBatchPutCallback).response(this.address, pointList, ignoreErrorsResult);
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
        if (multiFieldBatchPutCallback == null) { // 无回调逻辑，则失败打印日志。
            LOGGER.error("multi field no callback logic exception. address:" + this.address, ex);
        } else {
            multiFieldBatchPutCallback.failed(this.address, pointList, ex);
        }
    }

    /**
     * execute the retry logic when writing failed
     * @return true if retry actually executed; false if retry limit reached
     */
    private boolean errorRetry() {
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
        if (multiFieldBatchPutCallback != null) {
            retryCallback = httpResponseCallbackFactory.createMultiFieldBatchPutDataCallback(newAddress,
                    this.multiFieldBatchPutCallback, this.pointList, this.config, retryTimes, this.writeContext);
        } else {
            retryCallback = httpResponseCallbackFactory.createMultiFieldNoLogicBatchPutHttpFutureCallback(newAddress,
                    this.pointList, this.config, retryTimes, this.writeContext);
        }

        String jsonString = JSON.toJSONString(pointList);
        httpClient.post(HttpAPI.MPUT, jsonString, retryCallback);
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
            if (multiFieldBatchPutCallback == null) {
                LOGGER.error("multi field no callback logic exception.", ex);
            } else {
                multiFieldBatchPutCallback.failed(this.address, pointList, ex);
            }
        } finally {
            httpClient.getSemaphoreManager().release(address);
        }
    }
}