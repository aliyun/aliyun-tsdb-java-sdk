package com.aliyun.hitsdb.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.aliyun.hitsdb.client.value.request.Query;
import com.aliyun.hitsdb.client.value.request.SubQuery;
import com.aliyun.hitsdb.client.value.response.QueryResult;
import com.aliyun.hitsdb.client.value.type.Aggregator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.request.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHiTSDBClientQueryLimitAndDpValue {
    private static final Logger LOG = LoggerFactory.getLogger(TestHiTSDBClientQueryLimitAndDpValue.class);

    HiTSDB tsdb;

    @Before
    public void init() throws HttpClientInitException {
        HiTSDBConfig config = HiTSDBConfig.address("localhost", 8242).config();
        tsdb = HiTSDBClientFactory.connect(config);
    }

    @After
    public void after() {
        try {
            tsdb.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void queryAssert(HiTSDB tsdb,Query query,List<Point> putlist) throws InterruptedException {
        LOG.info("query = " + query.toString());
        List<QueryResult> queryResults = tsdb.query(query);
        contentAssert(putlist, queryResults);
    }

    public static void contentAssert(List<Point> putlist, List<QueryResult> queryResults) {

        if (queryResults.size() != 0) {
            int totalResultSize = 0;
            for (QueryResult result : queryResults) {
                totalResultSize += result.getDps().size();
            }

            if (putlist.size() != totalResultSize) {
                Assert.fail("put ("+putlist.size()+") And query size ("+queryResults.get(0).getDps().size()+") not same");
            }

        } else {
            if (putlist.size()!=0) {
                Assert.fail("put data points but query set is empty.");
            }
        }

        for (Point putPoint : putlist) {
            QueryResult queryResult = queryResults.get(0);
            if (!compareDouble((Number) (putPoint.getValue()), (Number) queryResult.getDps().get(putPoint.getTimestamp()))) {
                Assert.fail("put And query value not same");
            }
        }
    }

    public static boolean compareDouble(Number a, Number b) {
        return a.floatValue() == b.floatValue() ? true : false;
    }

    @Test
    public void testQueryLimitAndDpValue() throws InterruptedException {

        /** Query with limit and offset */
        {
            final String Metric = "testQueryLimitAndOffset";
            final String Tagk1 = "tagk1";
            final String Tagk2 = "tagk2";
            final String Tagv1 = "tagv1";
            final String Tagv2 = "tagv2";
            final int startTime = 1511927280;
            final int endTime = 1511928280;


            // 写入数据
            List<Point> putlist_ts1 = new ArrayList<Point>();
            List<Point> putlist_ts2 = new ArrayList<Point>();
            List<Point> checklist = new ArrayList<Point>(); // Used for query results comparing

            for (int i = 0; i < 1000; i++) {
                Point point = Point.metric(Metric).tag(Tagk1, Tagv1).timestamp(startTime + i)
                        .value(i).build();
                Point point2 = Point.metric(Metric).tag(Tagk2, Tagv2).timestamp(startTime + i)
                        .value(i + 1000).build();
                putlist_ts1.add(point);
                putlist_ts2.add(point2);
            }
            tsdb.putSync(putlist_ts1);
            tsdb.putSync(putlist_ts2);

            // Query with Aggregator None with Limit with Offset
            {
                int limit = 100;
                int offset = 100;
                checklist.clear();

                // Prepare query result checklist
                for (int i = offset; i < offset + limit; i++) {
                    Point point = Point.metric(Metric).tag(Tagk1, Tagv1).timestamp(startTime + i)
                            .value(i).build();
                    checklist.add(point);
                }
                Query query = Query.timeRange(startTime, endTime)
                        .sub(SubQuery.metric(Metric)
                                .aggregator(Aggregator.NONE)
                                .limit(limit)
                                .offset(offset)
                                .tag(Tagk1, Tagv1).build()).build();

                System.out.println("查询条件：" + query.toJSON());
                queryAssert(tsdb, query, checklist);
            }
        }

        /** Query With dpValue filtering */
        {
            final String Metric = "testQueryDpValuesFiltering";
            final String Tagk1 = "tagk1";
            final String Tagv1 = "tagv1";
            final int startTime = 1551927280;
            final int endTime = 1551928280;
            final int dps_size = endTime - startTime;

            // 写入数据
            List<Point> putlist_ts1 = new ArrayList<Point>();
            List<Point> checklist = new ArrayList<Point>(); // Used for query results comparing

            for (int i = 0; i < 1000; i++) {
                Point point = Point.metric(Metric).tag(Tagk1, Tagv1).timestamp(startTime + i)
                        .value(i).build();
                putlist_ts1.add(point);
            }
            tsdb.putSync(putlist_ts1);

            // Verify the data points have been successfully put into HiTSDB.
            {
                Query query_ts1 = Query.timeRange(startTime, endTime)
                        .sub(SubQuery.metric(Metric)
                                .aggregator(Aggregator.NONE)
                                .tag(Tagk1, Tagv1).build()).build();
                queryAssert(tsdb, query_ts1, putlist_ts1);
            }

            // Query with ">="
            {
                int value = 500;
                checklist.clear();

                // Prepare query result checklist
                for (int i = value; i < dps_size; i++) {
                    Point point = Point.metric(Metric).tag(Tagk1, Tagv1).timestamp(startTime + i)
                            .value(i).build();
                    checklist.add(point);
                }

                Query query = Query.timeRange(startTime, endTime)
                        .sub(SubQuery.metric(Metric)
                                .aggregator(Aggregator.NONE)
                                .dpValue(">=" + value)
                                .tag(Tagk1, Tagv1).build()).build();

                System.out.println("查询条件：" + query.toJSON());
                queryAssert(tsdb, query, checklist);
            }
        }
    }

    @Test
    public void testQueryGlobalLimit() throws InterruptedException {

        /** Query with limit and offset */
        {
            final String Metric = "testQueryGlobalLimit";
            final String Tagk1 = "tagk1";
            final String Tagk2 = "tagk2";
            final String Tagv1 = "tagv1";
            final String Tagv2 = "tagv2";
            final int startTime = 1511927280;
            final int endTime = 1511928280;


            // 写入数据
            List<Point> putlist_ts1 = new ArrayList<Point>();
            List<Point> putlist_ts2 = new ArrayList<Point>();
            List<Point> checklist1 = new ArrayList<Point>(); // Used for query results comparing
            List<Point> checklist2 = new ArrayList<Point>(); // Used for query results comparing

            for (int i = 0; i < 1000; i++) {
                Point point = Point.metric(Metric).tag(Tagk1, Tagv1).timestamp(startTime + i)
                        .value(i).build();
                Point point2 = Point.metric(Metric).tag(Tagk2, Tagv2).timestamp(startTime + i)
                        .value(i + 1000).build();
                putlist_ts1.add(point);
                putlist_ts2.add(point2);
            }
            tsdb.putSync(putlist_ts1);
            tsdb.putSync(putlist_ts2);

            // Query with Aggregator None with Limit with Offset with GlobalLimit
            {
                int limit = 100;
                int offset = 100;
                checklist1.clear();
                checklist2.clear();

                // Prepare query result checklist
                for (int i = offset; i < offset + limit / 2; i++) {
                    Point point1 = Point.metric(Metric).tag(Tagk1, Tagv1).timestamp(startTime + i)
                            .value(i).build();
                    Point point2 = Point.metric(Metric).tag(Tagk2, Tagv2).timestamp(startTime + i)
                            .value(i + 1000).build();
                    checklist1.add(point1);
                    checklist2.add(point2);
                }

                Query query = Query.timeRange(startTime, endTime)
                        .sub(SubQuery.metric(Metric)
                                .aggregator(Aggregator.NONE)
                                .limit(limit)
                                .globalLimit(limit)
                                .offset(offset).build()).build();

                System.out.println("查询条件：" + query.toJSON());
                List<QueryResult> queryResults = tsdb.query(query);
                Assert.assertEquals(2, queryResults.size());
                contentAssert(checklist1, (List<QueryResult>) queryResults.get(0));
                contentAssert(checklist2, (List<QueryResult>) queryResults.get(1));
            }
        }
    }
}
