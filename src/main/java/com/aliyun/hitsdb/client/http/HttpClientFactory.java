package com.aliyun.hitsdb.client.http;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;

import com.aliyun.hitsdb.client.Config;
import com.aliyun.hitsdb.client.util.Objects;
import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.http.semaphore.SemaphoreManager;

public class HttpClientFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientFactory.class);

    private static final String[] certDNList = {"*.tsdb.aliyuncs.com", "*.hitsdb.rds.aliyuncs.com"};

    public static HttpClient createHttpClient(Config config) throws HttpClientInitException {
        Objects.requireNonNull(config);

        // 创建 ConnectingIOReactor
        ConnectingIOReactor ioReactor = initIOReactorConfig(config);

        // 创建链接管理器
        PoolingNHttpClientConnectionManager cm;
        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }

                    @Override
                    public void checkClientTrusted(X509Certificate[] certs, String authType) {
                        // don't check
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] certs, String authType) throws CertificateException {

                        if (certs == null) {
                            throw new IllegalArgumentException("checkServerTrusted:x509Certificate array isnull");
                        }

                        if (!(certs.length > 0)) {
                            throw new IllegalArgumentException("checkServerTrusted: X509Certificate is empty");
                        }

                        if (!(null != authType && authType.contains("RSA"))) {
                            throw new CertificateException("checkServerTrusted: AuthType is not RSA");
                        }

                        for (X509Certificate cert : certs) {
                            cert.checkValidity();
                            if (!cert.getSubjectDN().getName().contains(certDNList[0])
                                    && !cert.getSubjectDN().getName().contains(certDNList[1])) {
                                throw new IllegalArgumentException("checkServerTrusted: host is invalid");
                            }
                        }
                    }
                }
        };
        HostnameVerifier hostnameVerifier = new HostnameVerifier() {
            @Override
            public boolean verify(String hostname, SSLSession session) {
                try {
                    String peerHost = session.getPeerHost();
                    X509Certificate[] peerCertificates = (X509Certificate[]) session
                            .getPeerCertificates();
                    for (X509Certificate certificate : peerCertificates) {
                        X500Principal subjectX500Principal = certificate
                                .getSubjectX500Principal();
                        String name = subjectX500Principal.getName();
                        String[] split = name.split(",");
                        for (String str : split) {
                            if (str.startsWith("CN")) {//证书绑定的域名或者ip
                                if (peerHost.equals(hostname) &&
                                        (str.contains(certDNList[0]) ||
                                                str.contains(certDNList[0]))) {
                                    return true;
                                }
                            }
                        }
                    }
                } catch (SSLPeerUnverifiedException e1) {
                    throw new IllegalArgumentException("host check failed: SSLPeerUnverifiedException");
                }
                return false;
            }
        };
        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustAllCerts, null);

            Registry<SchemeIOSessionStrategy> sessionStrategyRegistry =
                    RegistryBuilder.<SchemeIOSessionStrategy>create()
                            .register("http", NoopIOSessionStrategy.INSTANCE)
                            .register("https",
                                    new SSLIOSessionStrategy(sslContext, null,
                                            null, hostnameVerifier))
                            .build();
            cm = new PoolingNHttpClientConnectionManager(ioReactor, sessionStrategyRegistry);
        } catch (Exception e) {
            throw new HttpClientInitException();
        }

        // 创建令牌管理器
        SemaphoreManager semaphoreManager = createSemaphoreManager(config);

        // 创建HttpAsyncClient
        CloseableHttpAsyncClient httpAsyncClient = createPoolingHttpClient(config, cm);

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
            requestConfigBuilder.setConnectTimeout(httpConnectTimeout * 1000);
        }
        int httpSocketTimeout = config.getHttpSocketTimeout();
        if (httpSocketTimeout >= 0) {
            // SocketTimeout:Socket请求超时.数据传输过程中数据包之间间隔的最大时间.
            requestConfigBuilder.setSocketTimeout(httpSocketTimeout * 1000);
        }
        final int httpConnectionRequestTimeout = config.getHttpConnectionRequestTimeout();
        if (httpConnectionRequestTimeout >= 0) {
            // ConnectionRequestTimeout:httpclient使用连接池来管理连接，这个时间就是从连接池获取连接的超时时间，可以想象下数据库连接池
            requestConfigBuilder.setConnectionRequestTimeout(httpConnectionRequestTimeout * 1000);
        }
        return requestConfigBuilder.build();
    }

    private static ConnectingIOReactor initIOReactorConfig(Config config) {
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

    private static final AtomicInteger NUM = new AtomicInteger();

    private static ScheduledExecutorService initFixedCycleCloseConnection(final PoolingNHttpClientConnectionManager cm) {
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
                    cm.closeIdleConnections(3, TimeUnit.MINUTES);
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
            Config config, PoolingNHttpClientConnectionManager cm) throws HttpClientInitException {
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
        if (httpConnectionLiveTime > 0) {
            TSDBHttpAsyncCallbackExecutor httpAsyncCallbackExecutor = new TSDBHttpAsyncCallbackExecutor(httpConnectionLiveTime);
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
