package com.aliyun.hitsdb.client.http;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequests;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.message.BasicClassicHttpRequest;
import org.apache.hc.core5.http.nio.AsyncRequestProducer;
import org.apache.hc.core5.http.nio.AsyncResponseConsumer;
import org.apache.hc.core5.http.nio.CapacityChannel;
import org.apache.hc.core5.http.nio.support.BasicRequestProducer;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TestHttpAsyncClient {

    private CloseableHttpAsyncClient httpclient;

    @Before
    public void init() {
        httpclient = HttpAsyncClients.createDefault();
    }

    @BeforeClass
    public static void initClass() {
    }

    @AfterClass
    public static void afterClass() {
    }

    @After
    public void after() {
        try {
            httpclient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testHttpSyncClient() throws IOException {

        // Start the client
        httpclient.start();

        // Execute request
        final SimpleHttpRequest request1 = SimpleHttpRequests.get("http://www.apache.org/");
        Future<SimpleHttpResponse> future = httpclient.execute(request1, null);
        try {
            HttpResponse response1 = future.get();
            System.out.println(request1 + "->" + response1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testHttpAsyncClient() throws InterruptedException, IOException {
        CloseableHttpAsyncClient httpclient = HttpAsyncClients.createDefault();

        final CountDownLatch latch1 = new CountDownLatch(1);
        final SimpleHttpRequest request2 = SimpleHttpRequests.get("http://www.apache.org/");
        httpclient.execute(request2, new FutureCallback<SimpleHttpResponse>() {

            @Override
            public void completed(SimpleHttpResponse response2) {
                latch1.countDown();
                System.out.println(request2 + "->" + response2);
            }

            public void failed(final Exception ex) {
                latch1.countDown();
                System.out.println(request2 + "->" + ex);
            }

            public void cancelled() {
                latch1.countDown();
                System.out.println(request2 + " cancelled");
            }

        });

        latch1.await();
    }

    @Test
    public void testHttpAsyncClient2() throws InterruptedException, IOException {
        CloseableHttpAsyncClient httpclient = HttpAsyncClients.createDefault();
        // In real world one most likely would also want to stream
        // request and response body content
        final BasicClassicHttpRequest request2 = new BasicClassicHttpRequest(Method.GET.toString(), "http://www.apache.org/");
        final CountDownLatch latch2 = new CountDownLatch(1);
        AsyncRequestProducer producer = new BasicRequestProducer(request2, null);
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
            public void consumeResponse(HttpResponse response, EntityDetails entityDetails, HttpContext context, FutureCallback<HttpResponse> resultCallback) {
            }

            @Override
            public void informationResponse(HttpResponse response, HttpContext context) {
            }

            @Override
            public void failed(Exception cause) {
            }
        };
        httpclient.execute(producer, consumer, new FutureCallback<HttpResponse>() {

            public void completed(final HttpResponse response3) {
                latch2.countDown();
                System.out.println(request2 + "->" + response3);
            }

            public void failed(final Exception ex) {
                latch2.countDown();
                System.out.println(request2 + "->" + ex);
            }

            public void cancelled() {
                latch2.countDown();
                System.out.println(request2 + " cancelled");
            }

        });

        latch2.await();
    }

}
