package com.aliyun.hitsdb.client.callback.http;

import com.aliyun.hitsdb.client.Config;
import com.aliyun.hitsdb.client.HAPolicy;
import com.aliyun.hitsdb.client.http.HttpAddressManager;
import com.aliyun.hitsdb.client.http.HttpClient;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AbstractPutHttpResponseCallback implements FutureCallback<HttpResponse> {
    static final Logger LOGGER = LoggerFactory.getLogger(AbstractPutHttpResponseCallback.class);
    final int batchPutRetryTimes;
    final boolean compress;
    final HttpClient hitsdbHttpClient;
    final Config config;
    final String address;
    final HAPolicy.WriteContext writeContext;

    public AbstractPutHttpResponseCallback(String address, HttpClient httpclient, Config config, int batchPutRetryTimes, HAPolicy.WriteContext writeContext) {
        this.address = address;
        this.hitsdbHttpClient = httpclient;
        this.batchPutRetryTimes = batchPutRetryTimes;
        this.compress = config.isHttpCompress();
        this.config = config;
        this.writeContext = writeContext;
    }

    @Override
    public void completed(HttpResponse httpResponse) {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public void failed(Exception ex) {
        throw new UnsupportedOperationException("Not supported.");
    }


    @Override
    public void cancelled() {
        HttpClient httpClient = writeContext == null ? this.hitsdbHttpClient : writeContext.getClient();
        httpClient.getSemaphoreManager().release(this.address);
        LOGGER.info("the HttpAsyncClient has been cancelled");
    }

    protected String getNextAddress(HttpClient httpClient) {
        HttpAddressManager httpAddressManager = httpClient.getHttpAddressManager();
        String newAddress = httpAddressManager.getAddress();
        return newAddress;
    }

    protected HttpClient getHttpClient(StringBuffer newAddressBuff) {
        boolean acquire;
        HttpClient httpClient;
        int retryTimes = this.batchPutRetryTimes;
        if (writeContext != null) {
            if (writeContext.doWrite()) {
                // TODO: add retry interval
                writeContext.addRetryTimes();
                httpClient = writeContext.getClient();
                newAddressBuff.append(getNextAddress(httpClient));
            } else {
                httpClient = writeContext.getClient();
                httpClient.getSemaphoreManager().release(address);
                return null;
            }
        } else {
            httpClient = this.hitsdbHttpClient;
            while (true) {
                newAddressBuff.append(getNextAddress(httpClient));
                acquire = httpClient.getSemaphoreManager().acquire(newAddressBuff.toString());
                retryTimes--;
                if (acquire || retryTimes <= 0) {
                    break;
                }
            }

            if (retryTimes == 0) {
                httpClient.getSemaphoreManager().release(address);
                return null;
            }
        }
        return httpClient;
    }
}
