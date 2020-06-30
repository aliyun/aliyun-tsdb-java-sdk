package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.callback.AbstractBatchPutCallback;
import com.aliyun.hitsdb.client.callback.AbstractMultiFieldBatchPutCallback;
import com.aliyun.hitsdb.client.http.Host;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractConfig implements Config {

    protected List<Host> addresses = new ArrayList();

    /**
     * 写入请求限制数
     */
    protected int putRequestLimit;
    /**
     * 写入请求限制开关，true表示打开请求限制
     */
    protected boolean putRequestLimitSwitch;

    /**
     * 单值消费者队列长度
     */
    protected int batchPutBufferSize;

    /**
     * 多值消费者队列长度
     */
    protected int multiFieldBatchPutBufferSize;

    /**
     * 异步批量写回调接口
     */
    protected AbstractBatchPutCallback<?> batchPutCallback;

    /**
     * 多值异步批量写回调接口
     */
    protected AbstractMultiFieldBatchPutCallback<?> multiFieldBatchPutCallback;
    /**
     * 单值消费者线程数
     */
    protected int batchPutConsumerThreadCount;

    /**
     * 多值消费者线程数
     */
    protected int multiFieldBatchPutConsumerThreadCount;

    /**
     * 批量写入失败重试次数
     */
    protected int batchPutRetryCount;

    /**
     * 批量写入时最大点数
     */
    protected int batchPutSize;
    /**
     * 批量Put时从队列取数据时最长等待时间
     */
    protected int batchPutTimeLimit;

    /**
     * 最大写入TPS
     */
    protected int maxTPS;

    /**
     * TSDB endpoint
     */
    protected String host;

    /**
     * TSDB 端口号
     */
    protected int port;

    /**
     * 是否开启HTTP压缩
     */
    protected boolean httpCompress;

    /**
     * HTTP 连接池连接数
     */
    protected int httpConnectionPool;

    /**
     * HTTP连接超时时间
     */
    protected int httpConnectTimeout;

    /**
     * Socket 超时时间
     */
    protected int httpSocketTimeout;

    /**
     * 获取 HTTP 连接超时时间
     */
    protected int httpConnectionRequestTimeout;

    /**
     * HTTP连接不活动时存活时间
     */
    protected int httpConnectionLiveTime;

    /**
     * 是否使用HTTP长连接，0 表示短连接。-1表示长连接
     */
    protected int httpKeepaliveTime;

    /**
     * IO线程数
     */
    protected int ioThreadCount;

    /**
     * 是否使用反压模式写入
     */
    protected boolean backpressure;
    /**
     * 是否使用异步写
     */
    protected boolean asyncPut;

    /**
     * is https enable
     */
    protected boolean sslEnable;

    protected String authType;

    protected String instanceId;

    protected String tsdbUser;

    protected String basicPwd;

    protected byte[] certContent;

    @Override
    public boolean isSslEnable() {
        return sslEnable;
    }

    @Override
    public String getAuthType() {
        return authType;
    }

    @Override
    public String getInstanceId() {
        return instanceId;
    }

    @Override
    public String getTsdbUser() {
        return tsdbUser;
    }

    @Override
    public String getBasicPwd() {
        return basicPwd;
    }

    @Override
    public byte[] getCertContent() {
        return certContent;
    }

    @Override
    public int getPutRequestLimit() {
        return putRequestLimit;
    }

    @Override
    public int getBatchPutBufferSize() {
        return batchPutBufferSize;
    }

    @Override
    public AbstractBatchPutCallback<?> getBatchPutCallback() {
        return batchPutCallback;
    }

    @Override
    public void setBatchPutCallback(AbstractBatchPutCallback callback) {
        this.batchPutCallback = callback;
    }

    @Override
    public AbstractMultiFieldBatchPutCallback<?> getMultiFieldBatchPutCallback() {
        return this.multiFieldBatchPutCallback;
    }

    @Override
    public void setMultiFieldBatchPutCallback(AbstractMultiFieldBatchPutCallback callback) {
        this.multiFieldBatchPutCallback = callback;
    }

    @Override
    public int getBatchPutConsumerThreadCount() {
        return batchPutConsumerThreadCount;
    }

    @Override
    public int getMultiFieldBatchPutConsumerThreadCount() {
        return multiFieldBatchPutConsumerThreadCount;
    }

    @Override
    public int getBatchPutRetryCount() {
        return batchPutRetryCount;
    }

    @Override
    public int getBatchPutSize() {
        return batchPutSize;
    }

    @Override
    public int getBatchPutTimeLimit() {
        return batchPutTimeLimit;
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public List<Host> getAddresses() {
        return this.addresses;
    }

    @Override
    public int getHttpConnectionPool() {
        return httpConnectionPool;
    }

    @Override
    public int getHttpConnectTimeout() {
        return httpConnectTimeout;
    }

    @Override
    public int getHttpSocketTimeout() {
        return httpSocketTimeout;
    }

    @Override
    public int getHttpConnectionRequestTimeout() {
        return httpConnectionRequestTimeout;
    }

    @Override
    public int getIoThreadCount() {
        return ioThreadCount;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public boolean isPutRequestLimitSwitch() {
        return putRequestLimitSwitch;
    }

    @Override
    public boolean isHttpCompress() {
        return httpCompress;
    }

    @Override
    public boolean isBackpressure() {
        return backpressure;
    }

    @Override
    public int getHttpConnectionLiveTime() {
        return httpConnectionLiveTime;
    }

    @Override
    public int getHttpKeepaliveTime() {
        return httpKeepaliveTime;
    }

    @Override
    public boolean isAsyncPut() {
        return asyncPut;
    }

    @Override
    public int getMaxTPS() {
        return maxTPS;
    }

    @Override
    public int getMultiFieldBatchPutBufferSize() {
        return this.multiFieldBatchPutBufferSize;
    }

    protected void copy(AbstractConfig config, String host, int port) {
        config.host = host;
        config.port = port;
        config.batchPutCallback = this.batchPutCallback;
        config.multiFieldBatchPutCallback = this.multiFieldBatchPutCallback;
        config.batchPutSize = this.batchPutSize;
        config.batchPutTimeLimit = this.batchPutTimeLimit;
        config.batchPutBufferSize = this.batchPutBufferSize;
        config.multiFieldBatchPutBufferSize = this.multiFieldBatchPutBufferSize;
        config.batchPutRetryCount = this.batchPutRetryCount;
        config.httpConnectionPool = this.httpConnectionPool;
        config.httpConnectTimeout = this.httpConnectTimeout;
        config.httpSocketTimeout = this.httpSocketTimeout;
        config.httpConnectionRequestTimeout = this.httpConnectionRequestTimeout;
        config.putRequestLimitSwitch = this.putRequestLimitSwitch;
        config.putRequestLimit = this.putRequestLimit;
        config.batchPutConsumerThreadCount = this.batchPutConsumerThreadCount;
        config.multiFieldBatchPutConsumerThreadCount = this.multiFieldBatchPutConsumerThreadCount;
        config.httpCompress = this.httpCompress;
        config.ioThreadCount = this.ioThreadCount;
        config.backpressure = this.backpressure;
        config.httpConnectionLiveTime = this.httpConnectionLiveTime;
        config.httpKeepaliveTime = this.httpKeepaliveTime;
        config.maxTPS = this.maxTPS;
        config.asyncPut = this.asyncPut;
        if (this.putRequestLimitSwitch && this.putRequestLimit <= 0) {
            config.putRequestLimit = this.httpConnectionPool;
        }
    }
}
