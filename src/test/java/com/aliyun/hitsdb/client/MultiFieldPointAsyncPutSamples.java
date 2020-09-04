package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.callback.AbstractMultiFieldBatchPutCallback;
import com.aliyun.hitsdb.client.callback.MultiFieldBatchPutCallback;
import com.aliyun.hitsdb.client.callback.MultiFieldBatchPutDetailsCallback;
import com.aliyun.hitsdb.client.callback.MultiFieldBatchPutSummaryCallback;
import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;
import com.aliyun.hitsdb.client.value.response.batch.MultiFieldDetailsResult;
import com.aliyun.hitsdb.client.value.response.batch.SummaryResult;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiFieldPointAsyncPutSamples {


    public static void main(String[] args) throws IOException {
        // 异步写入使用普通回调方法
        putWithNormalCallback();
        // 异步写入使用详情回调方法
//        putWithDetailsCallback();
        // 异步写入使用摘要回调方法
//        putWithSummaryCallback();
    }

    public static void putWithNormalCallback() throws IOException {

        AbstractMultiFieldBatchPutCallback callback = new MultiFieldBatchPutCallback() {

            @Override
            public void response(String address, List<MultiFieldPoint> points, Result result) {
                int count = num.addAndGet(points.size());
                System.out.println(result);
                System.out.println("已处理" + count + "个点");
            }
            final AtomicInteger num = new AtomicInteger();
            @Override
            public void failed(String address, List<MultiFieldPoint> points, Exception ex) {
                System.err.println("业务回调出错！" + points.size() + " error!");
                ex.printStackTrace();
            }
        };
        putWithCallback(callback);
    }

    public static void putWithSummaryCallback() throws IOException {

        AbstractMultiFieldBatchPutCallback callback = new MultiFieldBatchPutSummaryCallback() {

            final AtomicInteger num = new AtomicInteger();
            @Override
            public void response(String address, List<MultiFieldPoint> points, SummaryResult result) {
                int count = num.addAndGet(points.size());
                System.out.println(result);
                System.out.println("已处理" + count + "个点");
            }



            @Override
            public void failed(String address, List<MultiFieldPoint> points, Exception ex) {
                System.err.println("业务回调出错！" + points.size() + " error!");
                ex.printStackTrace();
            }
        };
        putWithCallback(callback);
    }


    public static void putWithDetailsCallback() throws IOException {
        MultiFieldBatchPutDetailsCallback callback = new MultiFieldBatchPutDetailsCallback() {

            final AtomicInteger num = new AtomicInteger();

            @Override
            public void response(String address, List<MultiFieldPoint> points, MultiFieldDetailsResult result) {
                int count = num.addAndGet(points.size());
                System.out.println(result);
                System.out.println("已处理" + count + "个点");
            }

            @Override
            public void failed(String address, List<MultiFieldPoint> points, Exception ex) {
                System.err.println("业务回调出错！" + points.size() + " error!");
                ex.printStackTrace();
            }
        };
        putWithCallback(callback);
    }

    public static void putWithCallback(AbstractMultiFieldBatchPutCallback callback) throws IOException {
        TSDBConfig config = TSDBConfig.address("127.0.0.1", 8242)
                .httpConnectTimeout(90)
                // 批次写入时每次提交数据量
                .batchPutSize(500)
                // 单值异步写入线程数
                .batchPutConsumerThreadCount(1)
                // 多值异步写入缓存队列长度
                .multiFieldBatchPutBufferSize(10000)
                // 多值异步写入线程数
                .multiFieldBatchPutConsumerThreadCount(1)
                // 多值异步写入回调方法
                .listenMultiFieldBatchPut(callback)
                .config();

        // 特别注意，TSDB只需初始化一次即可
        TSDB tsdb = TSDBClientFactory.connect(config);

        MultiFieldPoint point = MultiFieldPoint
                .metric("test-test-test")
                .tag("a", "1")
                .tag("b", "2")
                .timestamp(System.currentTimeMillis())
                .field("f1", Math.random())
                .field("f2", Math.random())
                .build();
        tsdb.multiFieldPut(point);


        System.out.println("将要关闭");
        // 特别注意，在应用生命周期内不用不关闭TSDB实例，知道应用结束
        tsdb.close();
    }
}
