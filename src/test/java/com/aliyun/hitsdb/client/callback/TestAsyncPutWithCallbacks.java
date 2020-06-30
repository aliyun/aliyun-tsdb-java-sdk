package com.aliyun.hitsdb.client.callback;

import com.aliyun.hitsdb.client.TSDB;
import com.aliyun.hitsdb.client.TSDBClientFactory;
import com.aliyun.hitsdb.client.TSDBConfig;
import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.AbstractPoint;
import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;
import com.aliyun.hitsdb.client.value.request.Point;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Copyright @ 2020 alibaba.com
 * All right reserved.
 * Function：Test Async Put with Callbacks
 *
 * @author Benedict Jin
 * @since 2020/6/28
 */
public class TestAsyncPutWithCallbacks {

    private TSDB tsdb;

    @Test
    public void testAsyncPutWithCallbacksByTimeline() {

        final BatchPutCallback cb1 = new BatchPutCallback() {

            final AtomicInteger num = new AtomicInteger();

            @Override
            public void failed(String address, List<Point> points, Exception ex) {
                System.err.println("【业务1】回调出错！" + points.size() + " error!");
                ex.printStackTrace();
            }

            @Override
            public void response(String address, List<Point> input, Result output) {
                int count = num.addAndGet(input.size());
                System.out.println("【业务1】已处理" + count + "个点");
            }
        };
        final BatchPutCallback cb2 = new BatchPutCallback() {

            final AtomicInteger num = new AtomicInteger();

            @Override
            public void failed(String address, List<Point> points, Exception ex) {
                System.err.println("【业务2】回调出错！" + points.size() + " error!");
                ex.printStackTrace();
            }

            @Override
            public void response(String address, List<Point> input, Result output) {
                int count = num.addAndGet(input.size());
                System.out.println("【业务2】已处理" + count + "个点");
            }
        };
        final BatchPutCallback cb3 = new BatchPutCallback() {

            final AtomicInteger num = new AtomicInteger();

            @Override
            public void failed(String address, List<Point> points, Exception ex) {
                System.err.println("【业务3】回调出错！" + points.size() + " error!");
                ex.printStackTrace();
            }

            @Override
            public void response(String address, List<Point> input, Result output) {
                int count = num.addAndGet(input.size());
                System.out.println("【业务3】已处理" + count + "个点");
            }
        };

        final Map<Integer, AbstractBatchPutCallback<?>> cbs = new HashMap<Integer, AbstractBatchPutCallback<?>>();
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("tagk1", "tagv1");
        tags.put("tagk2", "tagv2");
        tags.put("tagk3", "tagv3");
        cbs.put(AbstractPoint.hashCode4Callback("test1", tags), cb1);
        cbs.put(AbstractPoint.hashCode4Callback("test2", null), cb2);
        tags = new HashMap<String, String>();
        tags.put("tagk1", "tagv1");
        tags.put("tagk2", "tagv2");
        cbs.put(AbstractPoint.hashCode4Callback("test3", tags), cb3);

        final TSDBConfig config = TSDBConfig
                .address("localhost", 8242)
                .listenBatchPuts(cbs)
                .httpCompress(true)
                .config();

        tsdb = TSDBClientFactory.connect(config);

        final Random random = new Random();
        final int baseTime = 1593399064;
        for (int i = 0; i < 1000; i++) {
            final int ts = baseTime + i;
            Point point = Point.metric("test1")
                    .tag("tagk1", "tagv1")
                    .tag("tagk2", "tagv2")
                    .tag("tagk3", "tagv3")
                    .timestamp(ts)
                    .value(random.nextDouble())
                    .build();
            System.out.println("put1:" + i);
            tsdb.put(point);

            point = Point.metric("test2")
                    .timestamp(ts)
                    .value(random.nextDouble())
                    .build();
            System.out.println("put2:" + i);
            tsdb.put(point);

            point = Point.metric("test3")
                    .tag("tagk1", "tagv1")
                    .tag("tagk2", "tagv2")
                    .timestamp(ts)
                    .value(random.nextDouble())
                    .build();
            System.out.println("put3:" + i);
            tsdb.put(point);
        }
    }

    @Test
    public void testAsyncPutWithCallbacksByMetric() {

        final BatchPutCallback cb1 = new BatchPutCallback() {

            final AtomicInteger num = new AtomicInteger();

            @Override
            public void failed(String address, List<Point> points, Exception ex) {
                System.err.println("【业务1】回调出错！" + points.size() + " error!");
                ex.printStackTrace();
            }

            @Override
            public void response(String address, List<Point> input, Result output) {
                int count = num.addAndGet(input.size());
                System.out.println("【业务1】已处理" + count + "个点");
            }
        };
        final BatchPutCallback cb2 = new BatchPutCallback() {

            final AtomicInteger num = new AtomicInteger();

            @Override
            public void failed(String address, List<Point> points, Exception ex) {
                System.err.println("【业务2】回调出错！" + points.size() + " error!");
                ex.printStackTrace();
            }

            @Override
            public void response(String address, List<Point> input, Result output) {
                int count = num.addAndGet(input.size());
                System.out.println("【业务2】已处理" + count + "个点");
            }
        };
        final BatchPutCallback cb3 = new BatchPutCallback() {

            final AtomicInteger num = new AtomicInteger();

            @Override
            public void failed(String address, List<Point> points, Exception ex) {
                System.err.println("【业务3】回调出错！" + points.size() + " error!");
                ex.printStackTrace();
            }

            @Override
            public void response(String address, List<Point> input, Result output) {
                int count = num.addAndGet(input.size());
                System.out.println("【业务3】已处理" + count + "个点");
            }
        };

        final Map<Integer, AbstractBatchPutCallback<?>> cbs = new HashMap<Integer, AbstractBatchPutCallback<?>>();
        cbs.put(AbstractPoint.hashCode4Callback("test11"), cb1);
        cbs.put(AbstractPoint.hashCode4Callback("test22"), cb2);
        cbs.put(AbstractPoint.hashCode4Callback("test33"), cb3);

        final TSDBConfig config = TSDBConfig
                .address("localhost", 8242)
                .listenBatchPuts(cbs)
                .callbacksByTimeLine(false)
                .httpCompress(true)
                .config();

        tsdb = TSDBClientFactory.connect(config);

        final Random random = new Random();
        final int baseTime = 1593399064;
        for (int i = 0; i < 1000; i++) {
            final int ts = baseTime + i;
            Point point = Point.metric("test11")
                    .tag("tagk1", "tagv1")
                    .tag("tagk2", "tagv2")
                    .tag("tagk3", "tagv3")
                    .timestamp(ts)
                    .value(random.nextDouble())
                    .build();
            System.out.println("put1:" + i);
            tsdb.put(point);

            point = Point.metric("test22")
                    .timestamp(ts)
                    .value(random.nextDouble())
                    .build();
            System.out.println("put2:" + i);
            tsdb.put(point);

            point = Point.metric("test33")
                    .tag("tagk1", "tagv1")
                    .tag("tagk2", "tagv2")
                    .timestamp(ts)
                    .value(random.nextDouble())
                    .build();
            System.out.println("put3:" + i);
            tsdb.put(point);
        }
    }

    @Test
    public void testAsyncMultiPutWithCallbacksByTimeline() {

        final MultiFieldBatchPutCallback cb1 = new MultiFieldBatchPutCallback() {

            final AtomicInteger num = new AtomicInteger();

            @Override
            public void failed(String address, List<MultiFieldPoint> request, Exception ex) {
                System.err.println("【业务1】回调出错！" + request.size() + " error!");
                ex.printStackTrace();
            }

            @Override
            public void response(String address, List<MultiFieldPoint> points, Result result) {
                int count = num.addAndGet(points.size());
                System.out.println("【业务1】已处理" + count + "个点");
            }
        };
        final MultiFieldBatchPutCallback cb2 = new MultiFieldBatchPutCallback() {

            final AtomicInteger num = new AtomicInteger();

            @Override
            public void failed(String address, List<MultiFieldPoint> request, Exception ex) {
                System.err.println("【业务2】回调出错！" + request.size() + " error!");
                ex.printStackTrace();
            }

            @Override
            public void response(String address, List<MultiFieldPoint> points, Result result) {
                int count = num.addAndGet(points.size());
                System.out.println("【业务2】已处理" + count + "个点");
            }
        };
        final MultiFieldBatchPutCallback cb3 = new MultiFieldBatchPutCallback() {

            final AtomicInteger num = new AtomicInteger();

            @Override
            public void failed(String address, List<MultiFieldPoint> request, Exception ex) {
                System.err.println("【业务3】回调出错！" + request.size() + " error!");
                ex.printStackTrace();
            }

            @Override
            public void response(String address, List<MultiFieldPoint> points, Result result) {
                int count = num.addAndGet(points.size());
                System.out.println("【业务3】已处理" + count + "个点");
            }
        };

        final Map<Integer, AbstractMultiFieldBatchPutCallback<?>> cbs = new HashMap<Integer, AbstractMultiFieldBatchPutCallback<?>>();
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("tagk1", "tagv1");
        tags.put("tagk2", "tagv2");
        tags.put("tagk3", "tagv3");
        cbs.put(AbstractPoint.hashCode4Callback("test1", tags), cb1);
        cbs.put(AbstractPoint.hashCode4Callback("test2", null), cb2);
        tags = new HashMap<String, String>();
        tags.put("tagk1", "tagv1");
        tags.put("tagk2", "tagv2");
        cbs.put(AbstractPoint.hashCode4Callback("test3", tags), cb3);

        final TSDBConfig config = TSDBConfig
                .address("localhost", 8242)
                .listenMultiFieldBatchPuts(cbs)
                .httpCompress(true)
                .config();

        tsdb = TSDBClientFactory.connect(config);

        final Random random = new Random();
        final int baseTime = 1593399064;
        for (int i = 0; i < 1000; i++) {
            final int ts = baseTime + i;
            MultiFieldPoint point = MultiFieldPoint.metric("test1")
                    .tag("tagk1", "tagv1")
                    .tag("tagk2", "tagv2")
                    .tag("tagk3", "tagv3")
                    .timestamp(ts)
                    .field("value", random.nextDouble())
                    .build();
            System.out.println("put1:" + i);
            tsdb.multiFieldPut(point);

            point = MultiFieldPoint.metric("test2")
                    .timestamp(ts)
                    .field("value", random.nextDouble())
                    .build();
            System.out.println("put2:" + i);
            tsdb.multiFieldPut(point);

            point = MultiFieldPoint.metric("test3")
                    .tag("tagk1", "tagv1")
                    .tag("tagk2", "tagv2")
                    .timestamp(ts)
                    .field("value", random.nextDouble())
                    .build();
            System.out.println("put3:" + i);
            tsdb.multiFieldPut(point);
        }
    }

    @Test
    public void testAsyncMultiPutWithCallbacksByMetric() {

        final MultiFieldBatchPutCallback cb1 = new MultiFieldBatchPutCallback() {

            final AtomicInteger num = new AtomicInteger();

            @Override
            public void failed(String address, List<MultiFieldPoint> request, Exception ex) {
                System.err.println("【业务1】回调出错！" + request.size() + " error!");
                ex.printStackTrace();
            }

            @Override
            public void response(String address, List<MultiFieldPoint> points, Result result) {
                int count = num.addAndGet(points.size());
                System.out.println("【业务1】已处理" + count + "个点");
            }
        };
        final MultiFieldBatchPutCallback cb2 = new MultiFieldBatchPutCallback() {

            final AtomicInteger num = new AtomicInteger();

            @Override
            public void failed(String address, List<MultiFieldPoint> request, Exception ex) {
                System.err.println("【业务2】回调出错！" + request.size() + " error!");
                ex.printStackTrace();
            }

            @Override
            public void response(String address, List<MultiFieldPoint> points, Result result) {
                int count = num.addAndGet(points.size());
                System.out.println("【业务2】已处理" + count + "个点");
            }
        };
        final MultiFieldBatchPutCallback cb3 = new MultiFieldBatchPutCallback() {

            final AtomicInteger num = new AtomicInteger();

            @Override
            public void failed(String address, List<MultiFieldPoint> request, Exception ex) {
                System.err.println("【业务3】回调出错！" + request.size() + " error!");
                ex.printStackTrace();
            }

            @Override
            public void response(String address, List<MultiFieldPoint> points, Result result) {
                int count = num.addAndGet(points.size());
                System.out.println("【业务3】已处理" + count + "个点");
            }
        };

        final Map<Integer, AbstractMultiFieldBatchPutCallback<?>> cbs = new HashMap<Integer, AbstractMultiFieldBatchPutCallback<?>>();
        cbs.put(AbstractPoint.hashCode4Callback("test1"), cb1);
        cbs.put(AbstractPoint.hashCode4Callback("test2"), cb2);
        cbs.put(AbstractPoint.hashCode4Callback("test3"), cb3);

        final TSDBConfig config = TSDBConfig
                .address("localhost", 8242)
                .listenMultiFieldBatchPuts(cbs)
                .callbacksByTimeLine(false)
                .httpCompress(true)
                .config();

        tsdb = TSDBClientFactory.connect(config);

        final Random random = new Random();
        final int baseTime = 1593399064;
        for (int i = 0; i < 1000; i++) {
            final int ts = baseTime + i;
            MultiFieldPoint point = MultiFieldPoint.metric("test1")
                    .tag("tagk1", "tagv1")
                    .tag("tagk2", "tagv2")
                    .tag("tagk3", "tagv3")
                    .timestamp(ts)
                    .field("value", random.nextDouble())
                    .build();
            System.out.println("put1:" + i);
            tsdb.multiFieldPut(point);

            point = MultiFieldPoint.metric("test2")
                    .timestamp(ts)
                    .field("value", random.nextDouble())
                    .build();
            System.out.println("put2:" + i);
            tsdb.multiFieldPut(point);

            point = MultiFieldPoint.metric("test3")
                    .tag("tagk1", "tagv1")
                    .tag("tagk2", "tagv2")
                    .timestamp(ts)
                    .field("value", random.nextDouble())
                    .build();
            System.out.println("put3:" + i);
            tsdb.multiFieldPut(point);
        }
    }

    @After
    public void teardown() throws Exception {
        if (tsdb != null) {
            tsdb.close();
        }
    }
}
