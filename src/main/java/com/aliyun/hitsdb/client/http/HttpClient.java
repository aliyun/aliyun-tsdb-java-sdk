package com.aliyun.hitsdb.client.http;

import com.aliyun.hitsdb.client.Config;
import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.callback.http.HttpResponseCallbackFactory;
import com.aliyun.hitsdb.client.exception.http.HttpClientException;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.http.semaphore.SemaphoreManager;
import org.apache.commons.codec.binary.Base64;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequests;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicClassicHttpRequest;
import org.apache.hc.core5.http.nio.AsyncEntityProducer;
import org.apache.hc.core5.http.nio.AsyncRequestProducer;
import org.apache.hc.core5.http.nio.AsyncResponseConsumer;
import org.apache.hc.core5.http.nio.CapacityChannel;
import org.apache.hc.core5.http.nio.entity.AsyncEntityProducers;
import org.apache.hc.core5.http.nio.support.BasicRequestProducer;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.hc.core5.reactor.IOReactorStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

public class HttpClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClient.class);
    public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
    private static final String version = "0.1";

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

    /**
     * 空闲连接清理服务
     */
    private ScheduledExecutorService connectionGcService;

    /**
     * is https enable
     */
    private boolean sslEnable;
    private String authType;
    private String instanceId;
    private String tsdbUser;
    private String basicPwd;
    private byte[] certContent;

    public void setSslEnable(boolean sslEnable) {
        this.sslEnable = sslEnable;
    }

    HttpClient(Config config, CloseableHttpAsyncClient httpclient, SemaphoreManager semaphoreManager, ScheduledExecutorService connectionGcService)
            throws HttpClientInitException {
        this.host = config.getHost();
        this.port = config.getPort();
        this.httpCompress = config.isHttpCompress();
        this.httpclient = httpclient;
        this.semaphoreManager = semaphoreManager;
        this.httpAddressManager = HttpAddressManager.createHttpAddressManager(config);
        this.unCompletedTaskNum = new AtomicInteger(0);
        this.httpResponseCallbackFactory = new HttpResponseCallbackFactory(unCompletedTaskNum, this, this.httpCompress);
        this.connectionGcService = connectionGcService;
        this.sslEnable = config.isSslEnable();
        this.authType = config.getAuthType();
        this.instanceId = config.getInstanceId();
        this.tsdbUser = config.getTsdbUser();
        this.basicPwd = config.getBasicPwd();
        this.certContent = config.getCertContent();
    }

    public void close() throws IOException {
        this.close(false);
    }

    public void close(boolean force) throws IOException {
        // 关闭等待
        if (!force) {
            // 优雅关闭
            while (true) {
                if (IOReactorStatus.ACTIVE.equals(httpclient.getStatus())) { // 正在运行则等待
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

        connectionGcService.shutdownNow();

        // 关闭
        httpclient.close();
    }

    public HttpResponse delete(String apiPath, String json) throws HttpClientException {
        return execute(SimpleHttpRequests.delete(getUrl(apiPath)), json);
    }

    public void delete(String apiPath, String json, FutureCallback<HttpResponse> httpCallback) {
        executeCallback(new BasicClassicHttpRequest(Method.DELETE.toString(), getUrl(apiPath)), json, httpCallback);
    }

    private HttpResponse execute(SimpleHttpRequest request, String json) throws HttpClientException {
        if (json != null && json.length() > 0) {
            request.addHeader("Content-Type", "application/json");
//            if (!this.httpCompress) {
                request.setBody(json, ContentType.APPLICATION_JSON);
//            } else {
//                request.addHeader("Accept-Encoding", "gzip, deflate");
//                request.setEntity(generateGZIPCompressEntity(json));
//            }
        }

        if (authType != null && !authType.trim().equals("")) {
            setAuthHeader(request);
        }

        unCompletedTaskNum.incrementAndGet();
        Future<SimpleHttpResponse> future = httpclient.execute(request, null);
        try {
            HttpResponse httpResponse = future.get();
            int retry = 0;
            while (httpResponse.getCode() == HttpStatus.SC_TEMPORARY_REDIRECT
                    || httpResponse.getCode() == HttpStatus.SC_UNAUTHORIZED) {
                if (httpResponse.getCode() == HttpStatus.SC_TEMPORARY_REDIRECT) {
                    sslEnable = true;
                    httpResponse = redirectResponse(httpResponse, request, httpclient);
                } else if (httpResponse.getCode() == HttpStatus.SC_UNAUTHORIZED) {
                    LOGGER.info("need authentication.....");
                    setAuthHeader(request);
                    httpResponse = authResponse(request, httpclient);
                }
                retry++;
                if (retry >= 10) {
                    break;
                }
            }
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

    public static HttpResponse redirectResponse(HttpResponse httpResponse,
                                                SimpleHttpRequest request,
                                                CloseableHttpAsyncClient httpclient)
            throws ExecutionException, InterruptedException {
        HttpResponse result = null;

        Header[] headers = httpResponse.getHeaders(HttpHeaders.LOCATION);
        for (Header header : headers) {
            if (header.getName().equalsIgnoreCase(HttpHeaders.LOCATION)) {
                String newUrl = header.getValue();
                request.setUri(URI.create(newUrl));
                Future<SimpleHttpResponse> future = httpclient.execute(request, null);
                result = future.get();
                break;
            }
        }
        if (result == null) {
            return httpResponse;
        }
        return result;
    }

    public void checkAuthInfo() {
        if (HiTSDBConfig.BASICTYPE.equalsIgnoreCase(authType)) {
            //if (instanceId == null || instanceId.trim().equals("")) {
            //    if (!host.startsWith("ts-")) {
            //        throw new HttpClientException("sorry, authentication need instance id");
            //    }
            //}
            if (tsdbUser == null || tsdbUser.trim().equals("")) {
                throw new HttpClientException("sorry, basic authentication need user name");
            }
            if (basicPwd == null || basicPwd.trim().equals("")) {
                throw new HttpClientException("sorry, basic authentication need user password");
            }
        } else if (HiTSDBConfig.ALITYPE.equalsIgnoreCase(authType)) {
            if (instanceId == null || instanceId.trim().equals("")) {
                if (!host.startsWith("ts-")) {
                    throw new HttpClientException("sorry, authentication need instance id");
                }
            }
            if (tsdbUser == null || tsdbUser.trim().equals("")) {
                throw new HttpClientException("sorry, ali authentication need user name");
            }
            if (certContent == null || certContent.length == 0) {
                throw new HttpClientException("sorry, ali authentication need cert content");
            }
            String certCStr = new String(certContent);
            if (certCStr.trim().equals("")) {
                throw new HttpClientException("sorry, ali authentication need cert content");
            }
        } else {
            throw new HttpClientException("sorry, authentication type unknown");
        }
    }

    public void setAuthHeader(HttpRequest request) {
        checkAuthInfo();
        if (Config.BASICTYPE.equalsIgnoreCase(authType)) {
            String auth = (instanceId == null || instanceId.trim().equals("")) ?
                    tsdbUser + ":" + basicPwd :
                    tsdbUser + "@" + instanceId + ":" + basicPwd;
            byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("US-ASCII")));
            String authHeader = authType + " " + new String(encodedAuth);
            request.removeHeaders(HttpHeaders.AUTHORIZATION);
            request.addHeader(HttpHeaders.AUTHORIZATION, authHeader);
        } else if (Config.ALITYPE.equalsIgnoreCase(authType)) {
            String auth = (instanceId == null || instanceId.trim().equals("")) ?
                    version + ":" + tsdbUser + ":" + Base64.encodeBase64String(certContent) :
                    version + ":" + tsdbUser + "@" + instanceId + ":" + Base64.encodeBase64String(certContent);
            byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("US-ASCII")));
            String authHeader = authType + " " + new String(encodedAuth);
            request.removeHeaders(HttpHeaders.AUTHORIZATION);
            request.addHeader(HttpHeaders.AUTHORIZATION, authHeader);
        }
    }

    public static HttpResponse authResponse(SimpleHttpRequest request,
                                            CloseableHttpAsyncClient httpclient)
            throws ExecutionException, InterruptedException {
        Future<SimpleHttpResponse> future = httpclient.execute(request, null);
        return future.get();
    }

    private void executeCallback(ClassicHttpRequest request, String json, final FutureCallback<HttpResponse> httpCallback) {
        if (json != null && json.length() > 0) {
            request.addHeader("Content-Type", "application/json");
            if (!this.httpCompress) {
                request.setEntity(generateStringEntity(json));
            } else {
                request.addHeader("Accept-Encoding", "gzip, deflate");
                request.setEntity(generateGZIPCompressEntity(json));
            }
        }

        if (authType != null && !authType.trim().equals("")) {
            setAuthHeader(request);
        }

        FutureCallback<HttpResponse> responseCallback = null;
        if (httpCallback != null) {
            unCompletedTaskNum.incrementAndGet();
            responseCallback = this.httpResponseCallbackFactory.wrapUpBaseHttpFutureCallback(httpCallback);
        }
        AsyncEntityProducer asyncEntityProducer = null;
        if (json != null && json.length() > 0) {
            asyncEntityProducer = AsyncEntityProducers.create(json, ContentType.APPLICATION_JSON);
        }
        AsyncRequestProducer producer = new BasicRequestProducer(request, asyncEntityProducer);
        AsyncResponseConsumer<HttpResponse> consumer = new AsyncResponseConsumer<HttpResponse>() {
            @Override
            public void releaseResources() {
            }

            @Override
            public void updateCapacity(CapacityChannel capacityChannel) {
            }

            @Override
            public void consume(ByteBuffer src) {
            }

            @Override
            public void streamEnd(List<? extends Header> trailers) {
            }

            @Override
            public void consumeResponse(HttpResponse response, EntityDetails entityDetails, HttpContext context, FutureCallback<HttpResponse> resultCallback) throws HttpException, IOException {
                if (httpCallback != null) {
                    httpCallback.completed(response);
                }
            }

            @Override
            public void informationResponse(HttpResponse response, HttpContext context) {
            }

            @Override
            public void failed(Exception cause) {
            }
        };
        httpclient.execute(producer, consumer, responseCallback);
    }

    private HttpEntity generateStringEntity(String json) {
        return new StringEntity(json, Charset.forName("UTF-8"));
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
            if (gzip != null) {
                try {
                    gzip.close();
                } catch (IOException e) {
                    throw new HttpClientException(e);
                }
            }
        }

        return new ByteArrayEntity(baos.toByteArray(), ContentType.APPLICATION_JSON, "gzip");
    }

    public HttpResponse get(String apiPath, String json) throws HttpClientException {
        return execute(SimpleHttpRequests.get(getUrl(apiPath)), json);
    }

    public void get(String apiPath, String json, FutureCallback<HttpResponse> httpCallback) {
        executeCallback(new BasicClassicHttpRequest(Method.GET.toString(), getUrl(apiPath)), json, httpCallback);
    }

    public HttpResponseCallbackFactory getHttpResponseCallbackFactory() {
        return httpResponseCallbackFactory;
    }

    private String getUrl(String apiPath) {
        if (sslEnable) {
            return "https://" + this.httpAddressManager.getAddress() + apiPath;
        } else {
            return "http://" + this.httpAddressManager.getAddress() + apiPath;
        }
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
        executeCallback(new BasicClassicHttpRequest(Method.POST.toString(), uri), json, httpCallback);
    }

    public void postToAddress(String address, String apiPath, String json, Map<String, String> params, FutureCallback<HttpResponse> httpCallback) {
        String httpFullAPI;
        if (sslEnable) {
            httpFullAPI = "https://" + address + apiPath;
        } else {
            httpFullAPI = "http://" + address + apiPath;
        }
        URI uri = createURI(httpFullAPI, params);
        executeCallback(new BasicClassicHttpRequest(Method.POST.toString(), uri), json, httpCallback);
    }

    public HttpResponse post(String apiPath, String json, Map<String, String> params) throws HttpClientException {
        String httpFullAPI = getUrl(apiPath);
        URI uri = createURI(httpFullAPI, params);
        return execute(SimpleHttpRequests.post(uri), json);
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
        return execute(SimpleHttpRequests.put(getUrl(apiPath)), json);
    }

    public void put(String apiPath, String json, FutureCallback<HttpResponse> httpCallback) {
        executeCallback(new BasicClassicHttpRequest(Method.PUT.toString(), getUrl(apiPath)), json, httpCallback);
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
