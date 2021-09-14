package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;
import org.junit.Test;

public class TestLindormTSDBClientFactory {

    @Test
    public void testConnectWithHostAndPort() {
        TSDB tsdb = LindormTSDBClientFactory.connect("127.0.0.1", 3002);
        MultiFieldPoint multiFieldPoint = generateMultiFieldPoint();
        tsdb.multiFieldPutSync(multiFieldPoint);
    }

    @Test
    public void testConnectWithConfig() {
        TSDBConfig config = TSDBConfig.address("127.0.0.1", 3002).config();
        TSDB tsdb = TSDBClientFactory.connect(config);

        MultiFieldPoint multiFieldPoint = generateMultiFieldPoint();
        tsdb.multiFieldPutSync(multiFieldPoint);
    }

    public MultiFieldPoint generateMultiFieldPoint() {
        final String metric = "wind";
        final String field = "speed";
        long timestamp = 1537170208;
        MultiFieldPoint multiFieldPoint = MultiFieldPoint.metric(metric)
                .field(field, 20)
                .timestamp(timestamp)
                .build();
        return multiFieldPoint;
    }
}
