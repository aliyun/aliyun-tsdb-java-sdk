package com.aliyun.hitsdb.client.callback.http;

import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.callback.LoadCallback;
import com.aliyun.hitsdb.client.exception.http.HttpServerErrorException;
import com.aliyun.hitsdb.client.exception.http.HttpServerNotSupportException;
import com.aliyun.hitsdb.client.exception.http.HttpUnknowStatusException;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.http.response.HttpStatus;
import com.aliyun.hitsdb.client.http.response.ResultResponse;
import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.CompressionBatchPoints;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadHttpResponseCallback implements FutureCallback<HttpResponse> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchPutHttpResponseCallback.class);
    private final LoadCallback loadCallback;
    private final CompressionBatchPoints points;
    private final boolean compress;
    private final HttpClient hitsdbHttpClient;
    private final String address;

    public LoadHttpResponseCallback(String address,HttpClient httpclient, LoadCallback loadCallback,CompressionBatchPoints points, HiTSDBConfig config) {
        super();
        this.address = address;
        this.hitsdbHttpClient = httpclient;
        this.loadCallback = loadCallback;
        this.points = points;
        this.compress = config.isHttpCompress();
    }

    @Override
    public void completed(HttpResponse httpResponse) {
        // 处理响应
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.compress);
        HttpStatus httpStatus = resultResponse.getHttpStatus();
        switch (httpStatus) {
            case ServerSuccess:
            case ServerSuccessNoContent:
                // 正常释放Semaphor
                this.hitsdbHttpClient.getSemaphoreManager().release(address);
                
                if (loadCallback == null) {
                    return;
                }

                loadCallback.response(this.address, points, new Result());
                return;
            case ServerNotSupport: {
                // 服务器返回4xx错误
                // 正常释放Semaphor
                this.hitsdbHttpClient.getSemaphoreManager().release(address);
                HttpServerNotSupportException ex = new HttpServerNotSupportException(resultResponse);
                this.failedWithResponse(ex);
                return;
            }
            case ServerError: {
                // 服务器返回5xx错误
                // 正常释放Semaphor
                this.hitsdbHttpClient.getSemaphoreManager().release(address);
                HttpServerErrorException ex = new HttpServerErrorException(resultResponse);
                this.failedWithResponse(ex);
    
                return;
            }
            default: {
                HttpUnknowStatusException ex = new HttpUnknowStatusException(resultResponse);
                this.failedWithResponse(ex);
                return;
            }
        }
    }

    /**
     * 有响应的异常处理。
     * @param ex
     */
    private void failedWithResponse(Exception ex) {
        if (loadCallback == null) { // 无回调逻辑，则失败打印日志。
            LOGGER.error("No callback logic exception. address:" + this.address, ex);
        } else {
            loadCallback.failed(this.address, this.points, ex);
        }
    }
    
    @Override
    public void failed(Exception ex) {
        // 重试后释放semaphore许可
        this.hitsdbHttpClient.getSemaphoreManager().release(address);

        // 处理完毕，向逻辑层传递异常并处理。
        if (loadCallback == null) {
            LOGGER.error("No callback logic exception.", ex);
            return;
        } else {
            loadCallback.failed(this.address, this.points, ex);
        }
        
    }

    @Override
    public void cancelled() {
        this.hitsdbHttpClient.getSemaphoreManager().release(this.address);
        LOGGER.info("the HttpAsyncClient has been cancelled");
    }

}
