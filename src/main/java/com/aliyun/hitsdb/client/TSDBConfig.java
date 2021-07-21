package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.callback.AbstractBatchPutCallback;
import com.aliyun.hitsdb.client.callback.AbstractMultiFieldBatchPutCallback;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.http.Host;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TSDBConfig extends AbstractConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(TSDBConfig.class);

    public static Builder address(String host) {
        return new Builder(host);
    }

    public static Builder address(String host, int port) {
        return new Builder(host, port);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public TSDBConfig copy(String host, int port) {
        TSDBConfig tsdbConfig = new TSDBConfig();
        copy(tsdbConfig, host, port);
        return tsdbConfig;
    }


    public static class Builder {
        private int putRequestLimit = -1;
        private boolean putRequestLimitSwitch = true;

        private int batchPutBufferSize = 10000;
        private int multiFieldBatchPutBufferSize = 10000;
        private AbstractBatchPutCallback<?> batchPutCallback;
        private AbstractMultiFieldBatchPutCallback<?> multiFieldBatchPutCallback;
        private int batchPutConsumerThreadCount = 1;
        private int multiFieldBatchPutConsumerThreadCount = 1;
        private int batchPutRetryCount = 0;
        private int batchPutSize = 500;
        private int batchPutTimeLimit = 300;
        private int maxTPS = -1;

        private String host;
        private int port = 8242;

        private boolean httpCompress = false;
        private int httpConnectionPool = 64; // 每个Host分配的连接数
        private int httpConnectTimeout = 90; // 单位：秒
        private int httpSocketTimeout = 90; // 单位：秒
        private int httpConnectionRequestTimeout = 90; // 单位：秒
        private int httpConnectionLiveTime = 0; // 单位：秒
        private int httpKeepaliveTime = -1; // 0 表示短连接。-1表示长连接。单位：秒。

        private int ioThreadCount = 1;
        private boolean backpressure = true;
        private boolean asyncPut = true;
        private HAPolicy haPolicy = null;

        private boolean sslEnable = false;
        private String authType;
        private String instanceId = null;
        private String tsdbUser = null;
        private String basicPwd = null;
        private String certPath = null;
        private byte[] certContent = null;


        private List<Host> addresses = new ArrayList();

        private Set<String> uniqueHost = new HashSet();

        private boolean deduplicationEnable = false;

        private boolean lastResultReverseEnable = false;

        public Builder(String host) {
            this.host = host;
            this.uniqueHost.add(host);
        }

        public Builder(String host, int port) {
            this.host = host;
            this.port = port;
            this.uniqueHost.add(host);
        }

        public Builder() {

        }

        public Builder putRequestLimit(int limit) {
            this.putRequestLimit = limit;
            this.putRequestLimitSwitch = true;
            return this;
        }

        public Builder addAddress(String host, int port) {
            String key = host + ":" + port;
            if (uniqueHost.contains(key)) {
                return this;
            }
            this.addresses.add(new Host(host, port));
            this.uniqueHost.add(key);
            return this;
        }


        public Builder addAddress(String host) {
            return addAddress(host, port);
        }

        public Builder batchPutBufferSize(int batchPutBufferSize) {
            this.batchPutBufferSize = batchPutBufferSize;
            return this;
        }

        public Builder multiFieldBatchPutBufferSize(int multiFieldBatchPutBufferSize) {
            this.multiFieldBatchPutBufferSize = multiFieldBatchPutBufferSize;
            return this;
        }

        public Builder batchPutConsumerThreadCount(int batchPutConsumerThreadCount) {
            this.batchPutConsumerThreadCount = batchPutConsumerThreadCount;
            return this;
        }

        public Builder multiFieldBatchPutConsumerThreadCount(int batchPutConsumerThreadCount) {
            this.multiFieldBatchPutConsumerThreadCount = batchPutConsumerThreadCount;
            return this;
        }


        public Builder batchPutRetryCount(int batchPutRetryCount) {
            this.batchPutRetryCount = batchPutRetryCount;
            return this;
        }

        public Builder batchPutSize(int batchPutSize) {
            this.batchPutSize = batchPutSize;
            return this;
        }

        public Builder batchPutTimeLimit(int batchPutTimeLimit) {
            this.batchPutTimeLimit = batchPutTimeLimit;
            return this;
        }

        public Builder closePutRequestLimit() {
            this.putRequestLimitSwitch = false;
            return this;
        }

        public Builder closeBackpressure() {
            this.backpressure = false;
            return this;
        }

        public Builder backpressure(boolean backpressure) {
            this.backpressure = backpressure;
            return this;
        }

        public Builder httpConnectionLiveTime(int httpConnectionLiveTime) {
            this.httpConnectionLiveTime = httpConnectionLiveTime;
            return this;
        }

        public Builder httpKeepaliveTime(int httpKeepaliveTime) {
            this.httpKeepaliveTime = httpKeepaliveTime;
            return this;
        }

        public Builder readonly() {
            this.asyncPut = false;
            return this;
        }

        public Builder readonly(boolean readonly) {
            if (readonly) {
                this.asyncPut = false;
            }
            return this;
        }

        public Builder asyncPut(boolean asyncPut) {
            this.asyncPut = asyncPut;
            return this;
        }

        public Builder addHAPolicy(HAPolicy policy) {
            this.haPolicy = policy;
            return this;
        }

        public Builder maxTPS(int maxTPS) {
            this.maxTPS = maxTPS;
            return this;
        }

        public Builder enableSSL(boolean sslEnable) {
            this.sslEnable = sslEnable;
            return this;
        }

        /**
         * expose the basicAuth without the instanceId as the argument,
         * because the TSDB server does not support multi-tenant so far
         */
        public Builder basicAuth(String tsdbUser, String basicPwd) {
            return basicAuth(null, tsdbUser, basicPwd);
        }

        Builder basicAuth(String instanceId, String tsdbUser, String basicPwd) {
            this.authType = TSDBConfig.BASICTYPE;
            this.instanceId = instanceId;
            this.tsdbUser = tsdbUser;
            this.basicPwd = basicPwd;
            return this;
        }

        public Builder aliAuth(String instanceId, String tsdbUser, String aliAuthPath) {
            this.authType = TSDBConfig.ALITYPE;
            this.instanceId = instanceId;
            this.tsdbUser = tsdbUser;
            this.certPath = aliAuthPath;
            File file = new File(certPath);
            if (!file.exists()) {
                throw new HttpClientInitException();
            }
            try {
                InputStream is = new FileInputStream(file);
                certContent = new byte[is.available()];
                int i = is.read(certContent);
                if (certContent.length == 0) {
                    throw new HttpClientInitException();
                }
            } catch (FileNotFoundException e) {
                throw new HttpClientInitException();
            } catch (IOException e) {
                throw new HttpClientInitException();
            } catch (Exception e) {
                throw new HttpClientInitException();
            }
            return this;
        }

        public Builder httpCompress(boolean httpCompress) {
            this.httpCompress = httpCompress;
            return this;
        }

        public Builder httpConnectionPool(int connectionPool) {
            if (connectionPool <= 0) {
                throw new IllegalArgumentException("The ConnectionPool con't be less then 1");
            }
            httpConnectionPool = connectionPool;
            return this;
        }

        public Builder httpConnectTimeout(int httpConnectTimeout) {
            this.httpConnectTimeout = httpConnectTimeout;
            return this;
        }

        public Builder httpSocketTimeout(int httpSocketTimeout) {
            this.httpSocketTimeout = httpSocketTimeout;
            return this;
        }

        public Builder httpConnectionRequestTimeout(int httpConnectionRequestTimeout) {
            this.httpConnectionRequestTimeout = httpConnectionRequestTimeout;
            return this;
        }

        public Builder ioThreadCount(int ioThreadCount) {
            this.ioThreadCount = ioThreadCount;
            return this;
        }

        public Builder listenBatchPut(AbstractBatchPutCallback<?> cb) {
            this.batchPutCallback = cb;
            return this;
        }

        public Builder listenMultiFieldBatchPut(AbstractMultiFieldBatchPutCallback<?> cb) {
            this.multiFieldBatchPutCallback = cb;
            return this;
        }

        public Builder openHttpCompress() {
            this.httpCompress = true;
            return this;
        }

        public Builder deduplicationEnable () {
            this.deduplicationEnable = true;
            return this;
        }

        public Builder lastResultReverseEnable () {
            this.lastResultReverseEnable = true;
            return this;
        }

        public  TSDBConfig config() {
            if (multiFieldBatchPutConsumerThreadCount <= 0 && batchPutConsumerThreadCount <= 0) {
                throw new IllegalArgumentException("At least one of multiFieldBatchPutConsumerThreadCount and batchPutConsumerThreadCount is greater than 0");
            }

            if (multiFieldBatchPutBufferSize <= 0 && batchPutBufferSize <= 0) {
                throw new IllegalArgumentException("At least one of multiFieldBatchPutBufferSize and batchPutBufferSize is greater than 0");
            }

            if ((multiFieldBatchPutConsumerThreadCount > 0 && multiFieldBatchPutBufferSize <= 0)
                    ||(multiFieldBatchPutConsumerThreadCount <= 0 && multiFieldBatchPutBufferSize > 0)) {
                throw new IllegalArgumentException("Both multiFieldBatchPutConsumerThreadCount and multiFieldBatchPutBufferSize should greater than 0");
            }

            if ((batchPutConsumerThreadCount > 0 && batchPutBufferSize <= 0)
                    ||(batchPutConsumerThreadCount <= 0 && batchPutBufferSize > 0)) {
                throw new IllegalArgumentException("Both batchPutConsumerThreadCount and batchPutBufferSize should greater than 0");
            }
            TSDBConfig config = new TSDBConfig();
            config.host = this.host;
            config.port = this.port;
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

            config.addresses = this.addresses;
            if (this.putRequestLimitSwitch && this.putRequestLimit <= 0) {
                config.putRequestLimit = this.httpConnectionPool;
            }
            config.sslEnable = this.sslEnable;
            config.authType = this.authType;
            config.instanceId = this.instanceId;
            config.tsdbUser = this.tsdbUser;
            config.basicPwd = this.basicPwd;
            config.certContent = this.certContent;
            if (this.haPolicy != null) {
                String secondaryHost = this.haPolicy.getSecondaryHost();
                int secondaryPort = this.haPolicy.getSecondaryPort();
                if (secondaryHost.equals(this.host) && secondaryPort == this.port) {
                    LOGGER.warn("Primary cluster and secondary cluster should not have same host and port");
                }
            }
            config.haPolicy = this.haPolicy;
            config.deduplicationEnable = this.deduplicationEnable;
            config.lastResultReverseEnable = this.lastResultReverseEnable;

            return config;
        }
    }
}