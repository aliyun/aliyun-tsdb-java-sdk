package com.aliyun.hitsdb.client.http;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.aliyun.hitsdb.client.util.Objects;
import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.http.semaphore.SemaphoreManager;

public class HttpClientFactory {

	private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientFactory.class);
	
	private static SemaphoreManager semaphoreManager;

	public static HttpClient createHttpClient(HiTSDBConfig config) throws HttpClientInitException {
		Objects.requireNonNull(config);
		
		// 创建 ConnectingIOReactor
		ConnectingIOReactor ioReactor = initIOReactorConfig(config);
		
		// 创建链接管理器
		final PoolingNHttpClientConnectionManager cm = new PoolingNHttpClientConnectionManager(ioReactor);
		
		// 创建令牌管理器
		semaphoreManager = createSemaphoreManager(config);
		
		// 创建HttpAsyncClient
		CloseableHttpAsyncClient httpAsyncClient = createPoolingHttpClient(config,cm,semaphoreManager);

		// 启动定时调度
		ScheduledExecutorService connectionGcService = initFixedCycleCloseConnection(cm);

		// 组合生产HttpClientImpl
		HttpClient httpClientImpl = new HttpClient(config,httpAsyncClient,semaphoreManager,connectionGcService);
		
		return httpClientImpl;
	}
	

	private static RequestConfig initRequestConfig(HiTSDBConfig config) {
		RequestConfig requestConfig = null;

		// 设置请求
		int httpConnectTimeout = config.getHttpConnectTimeout();
		// 需要设置
		if (httpConnectTimeout >= 0) {
			RequestConfig.Builder requestConfigBuilder = RequestConfig.custom();
			// ConnectTimeout:连接超时.连接建立时间，三次握手完成时间.
			requestConfigBuilder.setConnectTimeout(httpConnectTimeout * 1000);
			// SocketTimeout:Socket请求超时.数据传输过程中数据包之间间隔的最大时间.
			requestConfigBuilder.setSocketTimeout(httpConnectTimeout * 1000);
			// ConnectionRequestTimeout:httpclient使用连接池来管理连接，这个时间就是从连接池获取连接的超时时间，可以想象下数据库连接池
			requestConfigBuilder.setConnectionRequestTimeout(httpConnectTimeout * 1000);
			requestConfig = requestConfigBuilder.build();
		}

		return requestConfig;
	}

	private static ConnectingIOReactor initIOReactorConfig(HiTSDBConfig config) {
		int ioThreadCount = config.getIoThreadCount();
		IOReactorConfig ioReactorConfig = IOReactorConfig.custom().setIoThreadCount(ioThreadCount).build();
		ConnectingIOReactor ioReactor;
		try {
			ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);
			return ioReactor;
		} catch (IOReactorException e) {
			throw new HttpClientInitException();
		}
	}
	
	private static ScheduledExecutorService initFixedCycleCloseConnection(final PoolingNHttpClientConnectionManager cm) {
		// 定时关闭所有空闲链接
		ScheduledExecutorService connectionGcService = Executors.newSingleThreadScheduledExecutor(
				new ThreadFactory() {
					
					@Override
					public Thread newThread(Runnable r) {
						Thread t = new Thread(r, "Fixed-Cycle-Close-Connection" );
						t.setDaemon(true);
						return t;
					}
				}
				
		);

		connectionGcService.scheduleAtFixedRate(new Runnable() {
		
			@Override
			public void run() {
				try {
					LOGGER.info("Close idle connections, fixed cycle operation");
					cm.closeIdleConnections(3, TimeUnit.MINUTES);
				} catch(Exception ex) {
					LOGGER.error("",ex);
				}
			}
		}, 30, 30, TimeUnit.SECONDS);

		return connectionGcService;
	}
	
	private static SemaphoreManager createSemaphoreManager(HiTSDBConfig config) {
		int httpConnectionPool = config.getHttpConnectionPool();
		SemaphoreManager semaphoreManager = null;
		if (httpConnectionPool > 0) {
			String host = config.getHost();
			int port = config.getPort();
			int putRequestLimit = config.getPutRequestLimit();
			String address = String.format("%s:%d", host,port);
			semaphoreManager = SemaphoreManager.create(address, putRequestLimit);
		}
		
		return semaphoreManager;
	}

	private static CloseableHttpAsyncClient createPoolingHttpClient(HiTSDBConfig config,PoolingNHttpClientConnectionManager cm,SemaphoreManager semaphoreManager) throws HttpClientInitException {
		int httpConnectionPool = config.getHttpConnectionPool();
		int httpConnectionLiveTime = config.getHttpConnectionLiveTime();
		int httpKeepaliveTime = config.getHttpKeepaliveTime();
		
		RequestConfig requestConfig = initRequestConfig(config);
		
		if (httpConnectionPool > 0) {
			cm.setMaxTotal(httpConnectionPool);
			cm.setDefaultMaxPerRoute(httpConnectionPool);
			cm.closeExpiredConnections();
		}

		HttpAsyncClientBuilder httpAsyncClientBuilder = HttpAsyncClients.custom();

		// 设置连接管理器
		httpAsyncClientBuilder.setConnectionManager(cm);

		// 设置RequestConfig
		if (requestConfig != null) {
			httpAsyncClientBuilder.setDefaultRequestConfig(requestConfig);
		}

		// 设置Keepalive
		if (httpKeepaliveTime > 0) {
			HiTSDBConnectionKeepAliveStrategy hiTSDBConnectionKeepAliveStrategy = new HiTSDBConnectionKeepAliveStrategy(httpConnectionLiveTime);
			httpAsyncClientBuilder.setKeepAliveStrategy(hiTSDBConnectionKeepAliveStrategy);
		} else if (httpKeepaliveTime == 0) {
			HiTSDBConnectionReuseStrategy hiTSDBConnectionReuseStrategy = new HiTSDBConnectionReuseStrategy();
			httpAsyncClientBuilder.setConnectionReuseStrategy(hiTSDBConnectionReuseStrategy);
		}

		// 设置连接自动关闭
		if(httpConnectionLiveTime > 0) {
			HiTSDBHttpAsyncCallbackExecutor httpAsyncCallbackExecutor = new HiTSDBHttpAsyncCallbackExecutor(httpConnectionLiveTime);
			httpAsyncClientBuilder.setEventHandler(httpAsyncCallbackExecutor);
		}

		CloseableHttpAsyncClient client = httpAsyncClientBuilder.build();
		return client;
	}
}

class HiTSDBConnectionKeepAliveStrategy implements ConnectionKeepAliveStrategy {

	private long time;

	public HiTSDBConnectionKeepAliveStrategy(long time) {
		super();
		this.time = time;
	}

	@Override
	public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
		return 1000 * time;
	}

}

class HiTSDBConnectionReuseStrategy implements ConnectionReuseStrategy {

	@Override
	public boolean keepAlive(HttpResponse response, HttpContext context) {
		return false;
	}

}
