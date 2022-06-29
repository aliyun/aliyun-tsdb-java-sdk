package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.callback.AbstractBatchPutCallback;
import com.aliyun.hitsdb.client.callback.AbstractMultiFieldBatchPutCallback;
import com.aliyun.hitsdb.client.http.Host;

import java.util.List;

public interface Config {
    String BASICTYPE = "Basic";    // it should be "Basic" according to RFC 2617
    String ALITYPE = "alibaba-signature";

    boolean isSslEnable();

    String getAuthType();

    String getInstanceId();

    String getTsdbUser();

    String getBasicPwd();

    byte[] getCertContent();

    int getPutRequestLimit();

    int getBatchPutBufferSize();

    int getMultiFieldBatchPutBufferSize();

    AbstractBatchPutCallback<?> getBatchPutCallback();

    void setBatchPutCallback(AbstractBatchPutCallback callback);

    AbstractMultiFieldBatchPutCallback<?> getMultiFieldBatchPutCallback();

    void setMultiFieldBatchPutCallback(AbstractMultiFieldBatchPutCallback callback);

    boolean isAsyncPut();

    int getBatchPutConsumerThreadCount();

    int getMultiFieldBatchPutConsumerThreadCount();

    int getBatchPutRetryCount();

    int getBatchPutSize();

    int getBatchPutTimeLimit();

    String getHost();

    int getPort();

    List<Host> getAddresses();

    int getHttpConnectionPool();

    int getHttpConnectTimeout();

    int getHttpSocketTimeout();

    int getHttpConnectionRequestTimeout();

    int getIoThreadCount();

    boolean isPutRequestLimitSwitch();

    boolean isHttpCompress();

    boolean isBackpressure();

    int getHttpConnectionLiveTime();

    int getHttpKeepaliveTime();

    int getMaxTPS();

    Config copy(String host, int port);

    HAPolicy getHAPolicy();

    boolean isDeduplicationEnable();

    boolean isLastResultReverseEnable();
}