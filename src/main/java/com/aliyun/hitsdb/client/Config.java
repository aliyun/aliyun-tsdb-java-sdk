package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.callback.AbstractBatchPutCallback;
import com.aliyun.hitsdb.client.callback.AbstractMultiFieldBatchPutCallback;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.http.Host;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    Map<Integer, AbstractBatchPutCallback<?>> getBatchPutCallbacks();

    void setBatchPutCallbacks(Map<Integer, AbstractBatchPutCallback<?>> batchPutCallbacks);

    AbstractMultiFieldBatchPutCallback<?> getMultiFieldBatchPutCallback();

    void setMultiFieldBatchPutCallback(AbstractMultiFieldBatchPutCallback callback);

    Map<Integer, AbstractMultiFieldBatchPutCallback<?>> getMultiFieldBatchPutCallbacks();

    void setMultiFieldBatchPutCallbacks(Map<Integer, AbstractMultiFieldBatchPutCallback<?>> multiFieldBatchPutCallbacks);

    boolean isAsyncPut();

    boolean isCallbacksByTimeLine();

    void setCallbacksByTimeLine(boolean callbacksByTimeLine);

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
}