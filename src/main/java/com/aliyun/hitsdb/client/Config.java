package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.callback.AbstractBatchPutCallback;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.http.Host;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public interface Config {

	boolean isSslEnable();

	String getAuthType();

	String getInstanceId();

	String getTsdbUser();

	String getBasicPwd();

	byte[] getCertContent();

	int getPutRequestLimit();

	int getBatchPutBufferSize();

	AbstractBatchPutCallback<?> getBatchPutCallback();

	int getBatchPutConsumerThreadCount();

	int getBatchPutRetryCount();

	int getBatchPutSize();

	int getBatchPutTimeLimit();

	String getHost();
	
	List<Host> getAddresses();

	int getHttpConnectionPool();

	int getHttpConnectTimeout();

	int getIoThreadCount();

	int getPort();

	boolean isPutRequestLimitSwitch();

	boolean isHttpCompress();

	boolean isBackpressure();

	int getHttpConnectionLiveTime();

	int getHttpKeepaliveTime();

	boolean isAsyncPut();

	int getMaxTPS();

	void setBatchPutCallback(AbstractBatchPutCallback callback);


	Config copy(String host, int port);
}