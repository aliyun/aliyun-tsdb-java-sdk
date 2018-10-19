package com.aliyun.hitsdb.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import com.aliyun.hitsdb.client.callback.AbstractBatchPutCallback;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;

public class HiTSDBConfig {

	public static final String BASICTYPE = "basic";
	public static final String ALITYPE = "alibaba-signature";
	
	public static class Builder {
		public static volatile boolean ProducerThreadSerializeSwitch = false;

		private int putRequestLimit = -1;
		private boolean putRequestLimitSwitch = true;

		private int batchPutBufferSize = 10000;
		private AbstractBatchPutCallback<?> batchPutCallback;
		private int batchPutConsumerThreadCount = 1;
		private int batchPutRetryCount = 0;
		private int batchPutSize = 500;
		private int batchPutTimeLimit = 300;
		private int maxTPS = -1;

		private String host;
		private int port = 8242;

		private boolean httpCompress = false;
		private int httpConnectionPool = 64; // 每个Host分配的连接数
		private int httpConnectTimeout = 90; // 单位：秒
		private int httpConnectionLiveTime = 0; // 单位：秒
		private int httpKeepaliveTime = -1; // 0 表示短连接。-1表示长连接。单位：秒。

		private int ioThreadCount = 1;
		private boolean backpressure = true;
		private boolean asyncPut = true;
		
		private boolean sslEnable = false;
		private String authType;
		private String instanceId = null;
		private String tsdbUser = null;
		private String basicPwd = null;
		private String certPath = null;
		private byte[] certContent = null;

		public Builder(String host) {
			this.host = host;
		}

		public Builder(String host, int port) {
			this.host = host;
			this.port = port;
		}

		public Builder putRequestLimit(int limit) {
			this.putRequestLimit = limit;
			this.putRequestLimitSwitch = true;
			return this;
		}

		public Builder batchPutBufferSize(int batchPutBufferSize) {
			this.batchPutBufferSize = batchPutBufferSize;
			return this;
		}

		public Builder batchPutConsumerThreadCount(int batchPutConsumerThreadCount) {
			this.batchPutConsumerThreadCount = batchPutConsumerThreadCount;
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
		
		public Builder maxTPS(int maxTPS) {
			this.maxTPS = maxTPS;
			return this;
		}

		public HiTSDBConfig config() {
			HiTSDBConfig hiTSDBConfig = new HiTSDBConfig();

			hiTSDBConfig.host = this.host;
			hiTSDBConfig.port = this.port;
			hiTSDBConfig.batchPutCallback = this.batchPutCallback;
			hiTSDBConfig.batchPutSize = this.batchPutSize;
			hiTSDBConfig.batchPutTimeLimit = this.batchPutTimeLimit;
			hiTSDBConfig.batchPutBufferSize = this.batchPutBufferSize;
			hiTSDBConfig.batchPutRetryCount = this.batchPutRetryCount;
			hiTSDBConfig.httpConnectionPool = this.httpConnectionPool;
			hiTSDBConfig.httpConnectTimeout = this.httpConnectTimeout;
			hiTSDBConfig.putRequestLimitSwitch = this.putRequestLimitSwitch;
			hiTSDBConfig.putRequestLimit = this.putRequestLimit;
			hiTSDBConfig.batchPutConsumerThreadCount = this.batchPutConsumerThreadCount;
			hiTSDBConfig.httpCompress = this.httpCompress;
			hiTSDBConfig.ioThreadCount = this.ioThreadCount;
			hiTSDBConfig.backpressure = this.backpressure;
			hiTSDBConfig.httpConnectionLiveTime = this.httpConnectionLiveTime;
			hiTSDBConfig.httpKeepaliveTime = this.httpKeepaliveTime;
			hiTSDBConfig.maxTPS = this.maxTPS;
			hiTSDBConfig.asyncPut = this.asyncPut;
			if (this.putRequestLimitSwitch && this.putRequestLimit <= 0) {
				hiTSDBConfig.putRequestLimit = this.httpConnectionPool;
			}
			hiTSDBConfig.sslEnable = this.sslEnable;
			hiTSDBConfig.authType = this.authType;
			hiTSDBConfig.instanceId = this.instanceId;
			hiTSDBConfig.tsdbUser = this.tsdbUser;
			hiTSDBConfig.basicPwd = this.basicPwd;
			hiTSDBConfig.certContent = this.certContent;
			return hiTSDBConfig;
		}
		
		public Builder enableSSL(boolean sslEnable) {
			this.sslEnable = sslEnable;
			return this;
		}

		public Builder basicAuth(String instanceId, String tsdbUser, String basicPwd) {
			this.authType = HiTSDBConfig.BASICTYPE;
			this.instanceId = instanceId;
			this.tsdbUser = tsdbUser;
			this.basicPwd = basicPwd;
			return this;
		}
		
		public Builder aliAuth(String instanceId, String tsdbUser, String aliAuthPath) {
			this.authType = HiTSDBConfig.ALITYPE;
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

		public Builder ioThreadCount(int ioThreadCount) {
			this.ioThreadCount = ioThreadCount;
			return this;
		}

		public Builder listenBatchPut(AbstractBatchPutCallback<?> cb) {
			this.batchPutCallback = cb;
			return this;
		}

		public Builder openHttpCompress() {
			this.httpCompress = true;
			return this;
		}

	}

	public static Builder address(String host) {
		return new Builder(host);
	}

	public static Builder address(String host, int port) {
		return new Builder(host, port);
	}

	private int putRequestLimit;
	private boolean putRequestLimitSwitch;
	private int batchPutBufferSize;
	private AbstractBatchPutCallback<?> batchPutCallback;
	private int batchPutConsumerThreadCount;
	private int batchPutRetryCount;
	private int batchPutSize;
	private int batchPutTimeLimit;
	private int maxTPS;
	
	private String host;
	
	private boolean httpCompress;
	private int httpConnectionPool;
	private int httpConnectTimeout;
	private int httpConnectionLiveTime;
	private int httpKeepaliveTime;

	private int ioThreadCount;
	private boolean backpressure;
	private boolean asyncPut;

	private int port;
	
	/**
	 * is https enable
	 */
	private boolean sslEnable;
	
	private String authType;
	
	private String instanceId;
	
	private String tsdbUser;
	
	private String basicPwd;
	
	private byte[] certContent;
	
	public boolean isSslEnable() {
		return sslEnable;
	}

	public String getAuthType() {
		return authType;
	}

	public String getInstanceId() {
		return instanceId;
	}

	public String getTsdbUser() {
		return tsdbUser;
	}

	public String getBasicPwd() {
		return basicPwd;
	}

	public byte[] getCertContent() {
		return certContent;
	}

	public int getPutRequestLimit() {
		return putRequestLimit;
	}

	public int getBatchPutBufferSize() {
		return batchPutBufferSize;
	}

	public AbstractBatchPutCallback<?> getBatchPutCallback() {
		return batchPutCallback;
	}

	public int getBatchPutConsumerThreadCount() {
		return batchPutConsumerThreadCount;
	}

	public int getBatchPutRetryCount() {
		return batchPutRetryCount;
	}

	public int getBatchPutSize() {
		return batchPutSize;
	}

	public int getBatchPutTimeLimit() {
		return batchPutTimeLimit;
	}

	public String getHost() {
		return host;
	}

	public int getHttpConnectionPool() {
		return httpConnectionPool;
	}

	public int getHttpConnectTimeout() {
		return httpConnectTimeout;
	}

	public int getIoThreadCount() {
		return ioThreadCount;
	}

	public int getPort() {
		return port;
	}

	public boolean isPutRequestLimitSwitch() {
		return putRequestLimitSwitch;
	}

	public boolean isHttpCompress() {
		return httpCompress;
	}

	public boolean isBackpressure() {
		return backpressure;
	}

	public int getHttpConnectionLiveTime() {
		return httpConnectionLiveTime;
	}

	public int getHttpKeepaliveTime() {
		return httpKeepaliveTime;
	}

	public boolean isAsyncPut() {
		return asyncPut;
	}

	public int getMaxTPS() {
		return maxTPS;
	}

}