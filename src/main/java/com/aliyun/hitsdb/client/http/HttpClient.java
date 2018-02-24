package com.aliyun.hitsdb.client.http;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.callback.http.HttpResponseCallbackFactory;
import com.aliyun.hitsdb.client.exception.http.HttpClientException;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.http.request.HttpDeleteWithEntity;
import com.aliyun.hitsdb.client.http.request.HttpGetWithEntity;
import com.aliyun.hitsdb.client.http.semaphore.SemaphoreManager;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpClient {
	private static final Logger LOGGER = LoggerFactory.getLogger(HttpClient.class);
	public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

	private String host;
	private int port;

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
	
	/**
	 * 实际的HttpClient
	 */
	private final CloseableHttpAsyncClient httpclient;

	/**
	 * 回调接口工厂
	 */
	private final HttpResponseCallbackFactory httpResponseCallbackFactory;

	/**
	 * 未完成任务数 for graceful close.
	 */
	private final AtomicInteger unCompletedTaskNum;

	/**
	 * 使用Semaphore管理器
	 */
	private final SemaphoreManager semaphoreManager;

	/**
	 * 使用Semaphore管理器
	 */
	private final HttpAddressManager httpAddressManager;

	/**
	 * 是否压缩
	 */
	private final boolean httpCompress;

	HttpClient(HiTSDBConfig config, CloseableHttpAsyncClient httpclient, SemaphoreManager semaphoreManager)
			throws HttpClientInitException {
		this.httpCompress = config.isHttpCompress();
		this.httpclient = httpclient;
		this.semaphoreManager = semaphoreManager;
		this.httpAddressManager = HttpAddressManager.createHttpAddressManager(config);
		this.unCompletedTaskNum = new AtomicInteger(0);
		this.httpResponseCallbackFactory = new HttpResponseCallbackFactory(unCompletedTaskNum, this, this.httpCompress);
	}

	public void close() throws IOException {
		this.close(false);
	}

	public void close(boolean force) throws IOException {
		// 关闭等待
		if (!force) {
			// 优雅关闭
			while (true) {
				if (httpclient.isRunning()) { // 正在运行则等待
					int i = this.unCompletedTaskNum.get();
					if (i == 0) {
						break;
					} else {
						try {
							// 轮询检查优雅关闭
							Thread.sleep(50);
							continue;
						} catch (InterruptedException e) {
							LOGGER.warn("The thread {} is Interrupted", Thread.currentThread().getName());
							continue;
						}
					}
				} else {
					// 已经不再运行则退出
					break;
				}
			}
		}

		// 关闭
		httpclient.close();
	}

	public HttpResponse delete(String apiPath, String json) throws HttpClientException {
		final HttpDeleteWithEntity request = new HttpDeleteWithEntity(getUrl(apiPath));
		return execute(request, json);
	}

	public void delete(String apiPath, String json, FutureCallback<HttpResponse> httpCallback) {
		final HttpDeleteWithEntity request = new HttpDeleteWithEntity(getUrl(apiPath));
		executeCallback(request, json, httpCallback);
	}

	private HttpResponse execute(HttpEntityEnclosingRequestBase request, String json) throws HttpClientException {
		if (json != null && json.length() > 0) {
			request.addHeader("Content-Type", "application/json");
			if (!this.httpCompress) {
				request.setEntity(generateStringEntity(json));
			} else {
				request.addHeader("Accept-Encoding", "gzip, deflate");
				request.setEntity(generateGZIPCompressEntity(json));
			}
		}

		unCompletedTaskNum.incrementAndGet();
		Future<HttpResponse> future = httpclient.execute(request, null);
		try {
			HttpResponse httpResponse = future.get();
			return httpResponse;
		} catch (InterruptedException e) {
			throw new HttpClientException(e);
		} catch (ExecutionException e) {
			throw new HttpClientException(e);
		} catch (UnsupportedOperationException e) {
			throw new HttpClientException(e);
		} finally {
			unCompletedTaskNum.decrementAndGet();
		}
	}
	
	private void executeCallback(HttpEntityEnclosingRequestBase request, byte[] data, FutureCallback<HttpResponse> httpCallback) {
        if (data != null && data.length > 0) {
            request.addHeader("Content-Type", "application/tsc-data");
            request.setEntity(generateBytesEntity(data));
        }

        FutureCallback<HttpResponse> responseCallback = null;
        if (httpCallback != null) {
            unCompletedTaskNum.incrementAndGet();
            responseCallback = this.httpResponseCallbackFactory.wrapUpBaseHttpFutureCallback(httpCallback);
        }

        httpclient.execute(request,responseCallback);
    }

	private void executeCallback(HttpEntityEnclosingRequestBase request, String json, FutureCallback<HttpResponse> httpCallback) {
		if (json != null && json.length() > 0) {
			request.addHeader("Content-Type", "application/json");
			if (!this.httpCompress) {
				request.setEntity(generateStringEntity(json));
			} else {
				request.addHeader("Accept-Encoding", "gzip, deflate");
				request.setEntity(generateGZIPCompressEntity(json));
			}
		}

		FutureCallback<HttpResponse> responseCallback = null;
		if (httpCallback != null) {
			unCompletedTaskNum.incrementAndGet();
			responseCallback = this.httpResponseCallbackFactory.wrapUpBaseHttpFutureCallback(httpCallback);
		}

		httpclient.execute(request,responseCallback);
	}
	
	private StringEntity generateStringEntity(String json) {
		StringEntity stringEntity = new StringEntity(json, Charset.forName("UTF-8"));
		return stringEntity;
	}
	
	private ByteArrayEntity generateBytesEntity(byte[] data) {
	    ByteArrayEntity byteArrayEntity = new ByteArrayEntity(data);
        return byteArrayEntity;
    }

	private ByteArrayEntity generateGZIPCompressEntity(String json) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		GZIPOutputStream gzip = null;
		try {
			gzip = new GZIPOutputStream(baos);
			gzip.write(json.getBytes(DEFAULT_CHARSET));
		} catch (IOException e) {
			throw new HttpClientException(e);
		} finally {
			if(gzip != null) {
				try {
					gzip.close();
				} catch (IOException e) {
					throw new HttpClientException(e);
				}
			}
		}

		ByteArrayEntity byteEntity = new ByteArrayEntity(baos.toByteArray());
		byteEntity.setContentType("application/json");
		byteEntity.setContentEncoding("gzip");

		return byteEntity;
	}

	public HttpResponse get(String apiPath, String json) throws HttpClientException {
		final HttpGetWithEntity request = new HttpGetWithEntity(getUrl(apiPath));
		return execute(request, json);
	}

	public void get(String apiPath, String json, FutureCallback<HttpResponse> httpCallback) {
		final HttpGetWithEntity request = new HttpGetWithEntity(getUrl(apiPath));
		executeCallback(request, json, httpCallback);
	}

	public HttpResponseCallbackFactory getHttpResponseCallbackFactory() {
		return httpResponseCallbackFactory;
	}

	private String getUrl(String apiPath) {
		/*
		String host = null;
		int port = 0;

		if (this.virtualDomainSwitch) {
			String virtualDomain = this.getHost();
			try {
				Host srvHost = VIPClient.srvHost(virtualDomain);
				host = srvHost.getIp();
				int vipPort = srvHost.getPort();
				if (vipPort >= 0) {
					port = vipPort;
				}
			} catch (Exception e) {
				throw new VIPClientException(e);
			}
		} else {
			host = this.getHost();
			port = this.getPort();
		}

		return "http://" + host + ":" + port + apiPath;
		*/
		return "http://" + this.httpAddressManager.getAddress() + apiPath;
	}

	public HttpResponse post(String apiPath, String json) throws HttpClientException {
		return this.post(apiPath, json, new HashMap<String, String>());
	}
	
	public void post(String apiPath, String json, FutureCallback<HttpResponse> httpCallback) {
		this.post(apiPath, json, null, httpCallback);
	}
	
	public void postToAddress(String address, String apiPath, String json, FutureCallback<HttpResponse> httpCallback) {
		this.postToAddress(address, apiPath, json, null, httpCallback);
	}

	public void post(String apiPath, String json, Map<String, String> params, FutureCallback<HttpResponse> httpCallback) {
		String httpFullAPI = getUrl(apiPath);
		URI uri = createURI(httpFullAPI, params);
		final HttpPost request = new HttpPost(uri);
		executeCallback(request, json, httpCallback);
	}
	
	public void post(String apiPath, byte[] data, Map<String, String> params, FutureCallback<HttpResponse> httpCallback) {
        String httpFullAPI = getUrl(apiPath);
        URI uri = createURI(httpFullAPI, params);
        final HttpPost request = new HttpPost(uri);
        executeCallback(request, data, httpCallback);
    }
	
	public void postToAddress(String address, String apiPath, String json, Map<String, String> params, FutureCallback<HttpResponse> httpCallback) {
		String httpFullAPI = "http://" + address + apiPath;
		URI uri = createURI(httpFullAPI, params);
		final HttpPost request = new HttpPost(uri);
		executeCallback(request, json, httpCallback);
	}
	
	public void postToAddress(String address, String apiPath, byte[] data, Map<String, String> params, FutureCallback<HttpResponse> httpCallback) {
        String httpFullAPI = "http://" + address + apiPath;
        URI uri = createURI(httpFullAPI, params);
        final HttpPost request = new HttpPost(uri);
        executeCallback(request, data, httpCallback);
    }
	
	public void postToAddress(String address, String apiPath, byte[] data, FutureCallback<HttpResponse> httpCallback) {
        String httpFullAPI = "http://" + address + apiPath;
        URI uri = createURI(httpFullAPI, null);
        final HttpPost request = new HttpPost(uri);
        executeCallback(request, data, httpCallback);
    }

	public HttpResponse post(String apiPath, String json, Map<String, String> params) throws HttpClientException {
		String httpFullAPI = getUrl(apiPath);
		URI uri = createURI(httpFullAPI, params);
		final HttpPost request = new HttpPost(uri);
		return execute(request, json);
	}

	private URI createURI(String httpFullAPI, Map<String, String> params) {
		URIBuilder builder;
		try {
			builder = new URIBuilder(httpFullAPI);
		} catch (URISyntaxException e) {
			throw new HttpClientException(e);
		}

		if (params != null && !params.isEmpty()) {
			for (Entry<String, String> entry : params.entrySet()) {
				builder.setParameter(entry.getKey(), entry.getValue());
			}
		}

		URI uri;
		try {
			uri = builder.build();
		} catch (URISyntaxException e) {
			throw new HttpClientException(e);
		}
		return uri;
	}

	public HttpResponse put(String apiPath, String json) throws HttpClientException {
		final HttpPut request = new HttpPut(getUrl(apiPath));
		return execute(request, json);
	}

	public void put(String apiPath, String json, FutureCallback<HttpResponse> httpCallback) {
		final HttpPost request = new HttpPost(getUrl(apiPath));
		executeCallback(request, json, httpCallback);
	}

	public void start() {
		this.httpclient.start();
	}

	public SemaphoreManager getSemaphoreManager() {
		return semaphoreManager;
	}

	public HttpAddressManager getHttpAddressManager() {
		return httpAddressManager;
	}

}
