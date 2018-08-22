package com.aliyun.hitsdb.client.performance;

import com.aliyun.hitsdb.client.HiTSDBHAClient;
import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.request.Query;
import com.aliyun.hitsdb.client.value.request.SubQuery;
import com.aliyun.hitsdb.client.value.response.QueryResult;
import com.aliyun.hitsdb.client.value.type.Aggregator;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by changrui on 2018/8/7.
 */
public class TestHAClient {
    private static final Logger logger = LoggerFactory.getLogger(TestHAClient.class);

    static private Point createPoint(int tag, long t, double value) {
        return Point.metric("metric").tag("tag", String.valueOf(tag)).value(t, value).build();
    }

    @Test
    public void testHA() {
        HiTSDBConfig config1 = HiTSDBConfig.address("127.0.0.1", 8246).config();
        HiTSDBConfig config2 = HiTSDBConfig.address("127.0.0.1", 8247).config();
        HiTSDBHAClient tsdb = new HiTSDBHAClient(config1, config2);

        long currentSecond = System.currentTimeMillis() / 1000;
        int value = 1;
        int tsid = 1;

        Point singlePoint = createPoint(tsid, currentSecond++, value++);

        List<Point> points = new ArrayList();
        Point point = createPoint(tsid, currentSecond++, value++);
        points.add(point);
        point = createPoint(tsid, currentSecond, value);
        points.add(point);

        try {
            Result result = tsdb.putSync(singlePoint);
            if (result.toString().compareTo("{}") != 0) {
                logger.error("putSync point result error: {}", result.toString());
            }

            result = tsdb.putSync(points);
            if (result.toString().compareTo("{}") != 0) {
                logger.error("putSync points result error: {}", result.toString());
            }

            Query query = Query.timeRange(currentSecond - 1, currentSecond + 1)
                    .sub(SubQuery.metric("metric").aggregator(Aggregator.AVG).build()).build();

            List<QueryResult> results = tsdb.query(query);
            if (results != null) {
                logger.info("Query result is: {}", results.get(0).getDps());
            } else {
                logger.error("Can't get query result!!!");
            }

            results = tsdb.querySlave(query);
            if (results != null) {
                logger.info("QuerySlave result is: {}", results.get(0).getDps());
            } else {
                logger.error("Can't get querySlave result!!!");
            }
        } catch (Exception e) {
            logger.error("HA Client test exception: {}", e.getMessage());
        }
    }
}
