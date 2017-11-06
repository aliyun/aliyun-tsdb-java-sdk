package com.alibaba.hitsdb.client.configuration;

import java.util.List;

import org.junit.Test;

import com.alibaba.hitsdb.client.HiTSDBConfig;
import com.alibaba.hitsdb.client.callback.BatchPutCallback;
import com.alibaba.hitsdb.client.value.Result;
import com.alibaba.hitsdb.client.value.request.Point;

public class TestConfiguration {

    @Test
    public void test() {
        @SuppressWarnings("unused")
        HiTSDBConfig config = HiTSDBConfig.address("127.0.0.1", 8242) // 地址，第一个参数可以是域名，IP，或者VIPServer的域名。
                .asyncPut(true)
                .readonly(false)
        			.batchPutBufferSize(20000) // 客户端缓冲队列长度，默认为10000。
                .batchPutConsumerThreadCount(2) // 缓冲队列消费线程数，默认为1。
                .batchPutSize(800) // 每次批次提交给客户端点的个数，默认为500。
                .batchPutTimeLimit(100) // 每次等待最大时间限制，单位为ms，默认为200。
                .httpConnectionPool(256) // 网络连接池大小，默认为10。
                .httpConnectTimeout(10) // HTTP等待时间，单位为s，默认为90。
                .ioThreadCount(2) // IO 线程数，默认为1。
                .batchPutRetryCount(2) // 异常重试次数。
                .putRequestLimit(100) // IO请求队列数，默认等于连接池数。不建议使用。
                .closePutRequestLimit() // 不限制请求队列数，若关闭可能导致OOM，不建议使用。
                .listenBatchPut(new BatchPutCallback() { // 批量Put回调接口
                    @Override
                    public void response(String address, List<Point> input, Result output) {
                    }
                }).config(); // 构造对象
    }
}
