package com.aliyun.hitsdb.client.http;

import com.aliyun.hitsdb.client.Config;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.http.semaphore.SemaphoreManager;
import com.aliyun.hitsdb.client.util.Objects;
import org.apache.hc.client5.http.ConnectionKeepAliveStrategy;
import org.apache.hc.client5.http.HttpRequestRetryStrategy;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.core5.http.ConnectionReuseStrategy;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class HttpClientFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientFactory.class);

    private static final String[] certDNList = {"*.tsdb.aliyuncs.com", "*.hitsdb.rds.aliyuncs.com"};

    public static HttpClient createHttpClient(Config config) throws HttpClientInitException {
        Objects.requireNonNull(config);

        // 创建 ConnectingIOReactor
        IOReactorConfig ioReactor = initIOReactorConfig(config);

        // 创建链接管理器
        PoolingAsyncClientConnectionManager cm;
        try {
            cm = PoolingAsyncClientConnectionManagerBuilder.create().build();
        } catch (Exception e) {
            throw new HttpClientInitException();
        }

        // 创建令牌管理器
        SemaphoreManager semaphoreManager = createSemaphoreManager(config);

        // 创建HttpAsyncClient
        CloseableHttpAsyncClient httpAsyncClient = createPoolingHttpClient(config, ioReactor, cm);

        // 启动定时调度
        ScheduledExecutorService connectionGcService = initFixedCycleCloseConnection(cm);

        // 组合生产HttpClientImpl
        HttpClient httpClientImpl = new HttpClient(config, httpAsyncClient, semaphoreManager, connectionGcService);

        return httpClientImpl;
    }


    private static RequestConfig initRequestConfig(Config config) {
        final RequestConfig.Builder requestConfigBuilder = RequestConfig.custom();
        int httpConnectTimeout = config.getHttpConnectTimeout();
        if (httpConnectTimeout >= 0) {
            // ConnectTimeout:连接超时.连接建立时间，三次握手完成时间.
            requestConfigBuilder.setConnectTimeout(Timeout.ofSeconds(httpConnectTimeout));
        }
        int httpSocketTimeout = config.getHttpSocketTimeout();
        if (httpSocketTimeout >= 0) {
            // SocketTimeout:Socket请求超时.数据传输过程中数据包之间间隔的最大时间.
            requestConfigBuilder.setResponseTimeout(Timeout.ofSeconds(httpSocketTimeout));
        }
        final int httpConnectionRequestTimeout = config.getHttpConnectionRequestTimeout();
        if (httpConnectionRequestTimeout >= 0) {
            // ConnectionRequestTimeout:httpclient使用连接池来管理连接，这个时间就是从连接池获取连接的超时时间，可以想象下数据库连接池
            requestConfigBuilder.setConnectionRequestTimeout(Timeout.ofSeconds(httpConnectionRequestTimeout));
        }
        return requestConfigBuilder.build();
    }

    private static IOReactorConfig initIOReactorConfig(Config config) {
        int ioThreadCount = config.getIoThreadCount();
        return IOReactorConfig.custom().setIoThreadCount(ioThreadCount).build();
    }

    private static final AtomicInteger NUM = new AtomicInteger();

    private static ScheduledExecutorService initFixedCycleCloseConnection(final PoolingAsyncClientConnectionManager cm) {
        // 定时关闭所有空闲链接
        ScheduledExecutorService connectionGcService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactory() {

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "Fixed-Cycle-Close-Connection-" + NUM.incrementAndGet());
                        t.setDaemon(true);
                        return t;
                    }
                }

        );

        connectionGcService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Close idle connections, fixed cycle operation");
                    }
                    cm.closeIdle(TimeValue.ofMinutes(3));
                } catch (Exception ex) {
                    LOGGER.error("", ex);
                }
            }
        }, 30, 30, TimeUnit.SECONDS);

        return connectionGcService;
    }

    private static SemaphoreManager createSemaphoreManager(Config config) {
        int httpConnectionPool = config.getHttpConnectionPool();
        SemaphoreManager semaphoreManager = null;
        if (httpConnectionPool > 0) {
            String host = config.getHost();
            int port = config.getPort();
            int putRequestLimit = config.getPutRequestLimit();
            String address = String.format("%s:%d", host, port);
            semaphoreManager = SemaphoreManager.create(address, putRequestLimit, config.isPutRequestLimitSwitch());
        }

        return semaphoreManager;
    }

    private static CloseableHttpAsyncClient createPoolingHttpClient(
            Config config, IOReactorConfig ioReactor, PoolingAsyncClientConnectionManager cm) throws HttpClientInitException {
        int httpConnectionPool = config.getHttpConnectionPool();
        int httpConnectionLiveTime = config.getHttpConnectionLiveTime();
        int httpKeepaliveTime = config.getHttpKeepaliveTime();

        RequestConfig requestConfig = initRequestConfig(config);

        if (httpConnectionPool > 0) {
            cm.setMaxTotal(httpConnectionPool);
            cm.setDefaultMaxPerRoute(httpConnectionPool);
            cm.closeExpired();
        }

        HttpAsyncClientBuilder httpAsyncClientBuilder = HttpAsyncClients.custom();
        httpAsyncClientBuilder.setIOReactorConfig(ioReactor);
        HttpRequestRetryStrategy retryStrategy = config.getRetryStrategy();
        if (retryStrategy != null) {
            httpAsyncClientBuilder.setRetryStrategy(retryStrategy);
        }

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

        return httpAsyncClientBuilder.build();
    }
}

class HiTSDBConnectionKeepAliveStrategy implements ConnectionKeepAliveStrategy {

    private long time;

    public HiTSDBConnectionKeepAliveStrategy(long time) {
        super();
        this.time = time;
    }

    @Override
    public TimeValue getKeepAliveDuration(HttpResponse response, HttpContext context) {
        return TimeValue.of(time, TimeUnit.SECONDS);
    }

}

class HiTSDBConnectionReuseStrategy implements ConnectionReuseStrategy {

    @Override
    public boolean keepAlive(HttpRequest request, HttpResponse response, HttpContext context) {
        return false;
    }
}
