package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.callback.BatchPutDetailsCallback;
import com.aliyun.hitsdb.client.callback.MultiFieldBatchPutDetailsCallback;
import com.aliyun.hitsdb.client.value.request.*;
import com.aliyun.hitsdb.client.value.response.batch.DetailsResult;
import com.aliyun.hitsdb.client.value.response.batch.MultiFieldDetailsResult;
import com.aliyun.hitsdb.client.value.type.Aggregator;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yunxing on 2022/2/25.
 */
public class TestClientHA {
    final AtomicInteger succNum = new AtomicInteger();
    final AtomicInteger failNum = new AtomicInteger();

    private MultiFieldBatchPutDetailsCallback getMultiFieldDetailsCallback() {
        MultiFieldBatchPutDetailsCallback callback = new MultiFieldBatchPutDetailsCallback() {

            @Override
            public void response(String address, List<MultiFieldPoint> points, MultiFieldDetailsResult result) {
                succNum.addAndGet(points.size());
                System.out.println("业务回调成功");
            }

            @Override
            public void failed(String address, List<MultiFieldPoint> points, Exception ex) {
                failNum.addAndGet(points.size());
                System.err.println("业务回调出错！");
                ex.printStackTrace();
            }
        };
        return callback;
    }

    private BatchPutDetailsCallback getDetailsCallback() {
        BatchPutDetailsCallback callback = new BatchPutDetailsCallback() {

            @Override
            public void response(String address, List<Point> points, DetailsResult result) {
                succNum.addAndGet(points.size());
                System.out.println("业务回调成功");
            }

            @Override
            public void failed(String address, List<Point> points, Exception ex) {
                failNum.addAndGet(points.size());
                System.err.println("业务回调出错！");
                ex.printStackTrace();
            }
        };
        return callback;
    }

    /**
     * @author yunxing
     * @description Test case for 4 query HA policy under HA switch :
     *                  SecondaryPreferred, PrimaryPreferred, Primary and Secondary.
     *              Attention: We must start up 2 cluster before testing, and follow the instruction during testing
     * @throws InterruptedException
     */
    @Test
    public void testHASwitch() throws InterruptedException {
        final String metric = "TestClientQueryHA";
        final String tagName = "_id";
        final String tagValue = "tagv";
        final String mainIp = "127.0.0.1";
        final int mainPort = 3002;
        final String secondaryIp = "127.0.0.1";
        final int secondaryPort = 3003;
        int checkCount = 1;
        int checkInterval = 20;
        HashMap<String, String> tags = new HashMap<String, String>() {{ put(tagName, tagValue);}};


        long currentSecond = System.currentTimeMillis() / 1000;
        int value = 1;
        Boolean failed = false;
        Point point = Point.metric(metric).tag(tagName, tagValue).value(currentSecond, value).build();
        MultiFieldPoint mpoint = MultiFieldPoint.metric(metric).tag(tagName, tagValue).timestamp(currentSecond).field("field", value).build();

        Query query = Query.timeRange(currentSecond - 0, currentSecond + 1)
                .sub(SubQuery.metric(metric).aggregator(Aggregator.NONE).tag(tags).build()).build();
        MultiFieldQuery mquery = MultiFieldQuery.timeRange(currentSecond - 0, currentSecond + 1)
                .sub(MultiFieldSubQuery.metric(metric).tags(tags)
                        .fieldsInfo(new MultiFieldSubQueryDetails.Builder("*", Aggregator.NONE).build())
                        .build())
                .build();
        LastPointQuery lastQuery = LastPointQuery
                .builder().backScan(0).msResolution(true)
                .sub(LastPointSubQuery.builder(metric, tags).build()).build();
        LastPointQuery mlastQuery = LastPointQuery
                .builder().backScan(0).msResolution(true)
                .sub(LastPointSubQuery.builder(metric, new ArrayList<String>() {{
                    add("*");
                }}, tags).build()).build();

        // STEP 1: 创建 主、备 tsdb client以及 PrimaryPreferred、Primary、SecondaryPreferred、Secondary四种策略的tsdb client

        TSDBConfig config = TSDBConfig.address(mainIp, mainPort)
                .listenMultiFieldBatchPut(getMultiFieldDetailsCallback())
                .listenBatchPut(getDetailsCallback())
                .config();
        TSDBClient tsdb1 = new TSDBClient(config);

        TSDBConfig config2 = TSDBConfig.address(secondaryIp, secondaryPort)
                .listenMultiFieldBatchPut(getMultiFieldDetailsCallback())
                .listenBatchPut(getDetailsCallback())
                .config();
        TSDBClient tsdb2 = new TSDBClient(config2);

        HAPolicy primaryPreferredPolicy = HAPolicy.addSecondaryCluster(secondaryIp, secondaryPort)
                .setQueryRetryRule(HAPolicy.RetryRule.PrimaryPreferred, 1)
                .setWriteRetryRule(HAPolicy.RetryRule.PrimaryPreferred, 1)
                .setFailoverRule(checkCount, checkInterval).build();
        TSDBConfig primaryPreferredConfig = TSDBConfig.address(mainIp, mainPort).addHAPolicy(primaryPreferredPolicy)
                .listenMultiFieldBatchPut(getMultiFieldDetailsCallback())
                .listenBatchPut(getDetailsCallback())
                .config();
        TSDBClient primaryPreferredTsdb = new TSDBClient(primaryPreferredConfig);

        HAPolicy primaryPolicy = HAPolicy.addSecondaryCluster(secondaryIp, secondaryPort)
                .setQueryRetryRule(HAPolicy.RetryRule.Primary, 1)
                .setWriteRetryRule(HAPolicy.RetryRule.Primary, 1)
                .setFailoverRule(checkCount, checkInterval).build();
        TSDBConfig primaryConfig = TSDBConfig.address(mainIp, mainPort).addHAPolicy(primaryPolicy)
                .listenMultiFieldBatchPut(getMultiFieldDetailsCallback())
                .listenBatchPut(getDetailsCallback())
                .config();
        TSDBClient primaryTsdb = new TSDBClient(primaryConfig);


        HAPolicy secondaryPreferredpolicy = HAPolicy.addSecondaryCluster(secondaryIp, secondaryPort)
                .setQueryRetryRule(HAPolicy.RetryRule.SecondaryPreferred, 1)
                .setWriteRetryRule(HAPolicy.RetryRule.SecondaryPreferred, 1)
                .setFailoverRule(checkCount, checkInterval).build();
        TSDBConfig secondaryPreferredConfig = TSDBConfig.address(mainIp, mainPort).addHAPolicy(secondaryPreferredpolicy)
                .listenMultiFieldBatchPut(getMultiFieldDetailsCallback())
                .listenBatchPut(getDetailsCallback())
                .config();
        TSDBClient secondaryPreferredTsdb = new TSDBClient(secondaryPreferredConfig);

        HAPolicy secondaryPolicy = HAPolicy.addSecondaryCluster(secondaryIp, secondaryPort)
                .setQueryRetryRule(HAPolicy.RetryRule.Secondary, 1)
                .setWriteRetryRule(HAPolicy.RetryRule.Secondary, 1)
                .setFailoverRule(checkCount, checkInterval).build();
        TSDBConfig secondaryConfig = TSDBConfig.address(mainIp, mainPort).addHAPolicy(secondaryPolicy)
                .listenMultiFieldBatchPut(getMultiFieldDetailsCallback())
                .listenBatchPut(getDetailsCallback())
                .config();
        TSDBClient secondaryTsdb = new TSDBClient(secondaryConfig);

        // STEP 2: 测试正常情况下单值、多值、同步、异步写,查询正常
        {
            succNum.set(0);
            failNum.set(0);

            testSuccess(tsdb1, point, mpoint, query, mquery, lastQuery, mlastQuery);

            testSuccess(tsdb2, point, mpoint, query, mquery, lastQuery, mlastQuery);

            testSuccess(primaryPreferredTsdb, point, mpoint, query, mquery, lastQuery, mlastQuery);

            testSuccess(primaryTsdb, point, mpoint, query, mquery, lastQuery, mlastQuery);

            testSuccess(secondaryPreferredTsdb, point, mpoint, query, mquery, lastQuery, mlastQuery);

            testSuccess(secondaryTsdb, point, mpoint, query, mquery, lastQuery, mlastQuery);

            Thread.sleep(5 * 1000);

            Assert.assertEquals(12, succNum.get());
            Assert.assertEquals(0, failNum.get());
        }


        // STEP 3: 测试主 fail
        {
            succNum.set(0);
            failNum.set(0);

            System.out.println("please kill primary cluster!");
            while (true) {
                try {
                    tsdb1.getVersionInfo();
                } catch (Exception e) {
                    break;
                }
                System.out.println("please kill primary cluster!");
                Thread.sleep(3 * 1000);
            }

            // tsdb1 失败,tsdb2 成功, 带主备切换配置的同步、异步写入和查询能成功执行
            testFailed(tsdb1, point, mpoint, query, mquery, lastQuery, mlastQuery);

            testSuccess(tsdb2, point, mpoint, query, mquery, lastQuery, mlastQuery);

            testSuccess(primaryPreferredTsdb, point, mpoint, query, mquery, lastQuery, mlastQuery);

            testFailed(primaryTsdb, point, mpoint, query, mquery, lastQuery, mlastQuery);

            testSuccess(secondaryPreferredTsdb, point, mpoint, query, mquery, lastQuery, mlastQuery);

            testSuccess(secondaryTsdb, point, mpoint, query, mquery, lastQuery, mlastQuery);

            Thread.sleep(5 * 1000);

            Assert.assertEquals(8, succNum.get());
            Assert.assertEquals(4, failNum.get());
        }


        // STEP 4: 测试主拉起，备 fail
        {
            succNum.set(0);
            failNum.set(0);

            while (true) {
                try {
                    tsdb1.getVersionInfo();
                    break;
                } catch (Exception e) {
                    System.out.println("please start up primary cluster!");
                    Thread.sleep(5 * 1000);
                }
            }

            System.out.println("please kill secondary cluster!");
            Thread.sleep(5 * 1000);
            while (true) {
                try {
                    tsdb2.getVersionInfo();
                } catch (Exception e) {
                    break;
                }
                System.out.println("please kill secondary cluster!");
                Thread.sleep(5 * 1000);
            }

            // tsdb1 成功,tsdb2 成功, 带主备切换配置的同步、异步写入和查询能成功执行

            testSuccess(tsdb1, point, mpoint, query, mquery, lastQuery, mlastQuery);

            testFailed(tsdb2, point, mpoint, query, mquery, lastQuery, mlastQuery);

            testSuccess(primaryPreferredTsdb, point, mpoint, query, mquery, lastQuery, mlastQuery);

            testSuccess(primaryTsdb, point, mpoint, query, mquery, lastQuery, mlastQuery);

            testSuccess(secondaryPreferredTsdb, point, mpoint, query, mquery, lastQuery, mlastQuery);

            testFailed(secondaryTsdb, point, mpoint, query, mquery, lastQuery, mlastQuery);

            Thread.sleep(5 * 1000);

            Assert.assertEquals(8, succNum.get());
            Assert.assertEquals(4, failNum.get());
        }
    }

    /**
     * @author yunxing
     * @description this testcase will not stop,
     *      kill primary cluster or secondary cluster in turn to check ha swtich
     * @throws InterruptedException
     */
    @Test
    public void testForeverWrite() throws InterruptedException {
        final String metric = "TestForeverWrite";
        final String tagName = "_id";
        final String tagValue = "tagv";
        final String mainIp = "127.0.0.1";
        final int mainPort = 3002;
        final String secondaryIp = "127.0.0.1";
        final int secondaryPort = 3003;
        int checkCount = 30;
        int checkInterval = 20;

        long currentSecond = System.currentTimeMillis() / 1000;
        int value = 1;
        HashMap<String, String> tags = new HashMap<String, String>() {{ put(tagName, tagValue);}};
        Point point = Point.metric(metric).tag(tagName, tagValue).value(currentSecond, value).build();
        MultiFieldPoint mpoint = MultiFieldPoint.metric(metric).tags(tags).timestamp(currentSecond).field("field", value).build();

        Query query = Query.timeRange(currentSecond - 0, currentSecond + 1)
                .sub(SubQuery.metric(metric).aggregator(Aggregator.NONE).tag(tags).build()).build();
        MultiFieldQuery mquery = MultiFieldQuery.timeRange(currentSecond - 0, currentSecond + 1)
                .sub(MultiFieldSubQuery.metric(metric).tags(tags)
                        .fieldsInfo(new MultiFieldSubQueryDetails.Builder("*", Aggregator.NONE).build())
                        .build())
                .build();
        LastPointQuery lastQuery = LastPointQuery
                .builder().backScan(0).msResolution(true)
                .sub(LastPointSubQuery.builder(metric, tags).build()).build();
        LastPointQuery mlastQuery = LastPointQuery
                .builder().backScan(0).msResolution(true)
                .sub(LastPointSubQuery.builder(metric, new ArrayList<String>() {{
                    add("*");
                }}, tags).build()).build();

        // STEP 1: TEST FOR HAPolicy.RetryRule.Secondary AND HAPolicy.RetryRule.SecondaryPreferred
        HAPolicy primaryPreferredPolicy = HAPolicy.addSecondaryCluster(secondaryIp, secondaryPort)
                .setQueryRetryRule(HAPolicy.RetryRule.PrimaryPreferred, 1)
                .setWriteRetryRule(HAPolicy.RetryRule.PrimaryPreferred, 1)
                .setFailoverRule(checkCount, checkInterval).build();


        TSDBConfig primaryPreferredConfig = TSDBConfig.address(mainIp, mainPort).addHAPolicy(primaryPreferredPolicy)
                .listenMultiFieldBatchPut(getMultiFieldDetailsCallback())
                .listenBatchPut(getDetailsCallback())
                .config();
        TSDBClient primaryPreferredTsdb = new TSDBClient(primaryPreferredConfig);

        HAPolicy SecondaryPreferredpolicy = HAPolicy.addSecondaryCluster(secondaryIp, secondaryPort)
                .setQueryRetryRule(HAPolicy.RetryRule.SecondaryPreferred, 1)
                .setWriteRetryRule(HAPolicy.RetryRule.SecondaryPreferred, 1)
                .setFailoverRule(checkCount, checkInterval).build();
        TSDBConfig SecondaryPreferredConfig = TSDBConfig.address(mainIp, mainPort)
                .listenMultiFieldBatchPut(getMultiFieldDetailsCallback())
                .listenBatchPut(getDetailsCallback())
                .addHAPolicy(SecondaryPreferredpolicy).config();
        TSDBClient secondaryPreferredTsdb = new TSDBClient(SecondaryPreferredConfig);



        while (true) {
            testSuccess(primaryPreferredTsdb, point, mpoint, query, mquery, lastQuery, mlastQuery);
            testSuccess(secondaryPreferredTsdb, point, mpoint, query, mquery, lastQuery, mlastQuery);
            Thread.sleep(1000);
            Assert.assertEquals(0, failNum.get());
        }
    }


    private void testSuccess(TSDBClient client, Point point, MultiFieldPoint mpoint, Query query, MultiFieldQuery mquery,
                             LastPointQuery lastQuery, LastPointQuery mlastQuery) {
        client.putSync(point);
        client.multiFieldPutSync(mpoint);
        client.put(point);
        client.multiFieldPut(mpoint);

        Assert.assertTrue(client.multiFieldQuery(mquery).size() > 0);
        Assert.assertTrue(client.query(query).size() > 0);
        Assert.assertTrue(client.queryLast(lastQuery).size() > 0);
        Assert.assertTrue(client.multiFieldQueryLast(mlastQuery).size() > 0);
    }

    private void testFailed(TSDBClient client, Point point, MultiFieldPoint mpoint, Query query, MultiFieldQuery mquery,
                            LastPointQuery lastQuery, LastPointQuery mlastQuery) {
        boolean failed = false;
        try {
            client.putSync(point);
        } catch (Exception e) {
            failed = true;
        }
        if (!failed) {
            Assert.fail();
        }
        failed = false;
        try {
            client.multiFieldPutSync(mpoint);
        } catch (Exception e) {
            failed = true;
        }
        if (!failed) {
            Assert.fail();
        }
        client.put(point);
        client.multiFieldPut(mpoint);

        failed = false;
        try {
            client.multiFieldQuery(mquery);
        } catch (Exception e) {
            failed = true;
        }
        if (!failed) {
            Assert.fail();
        }
        failed = false;
        try {
            client.query(query);
        } catch (Exception e) {
            failed = true;
        }
        if (!failed) {
            Assert.fail();
        }
        failed = false;
        try {
            client.queryLast(lastQuery);
        } catch (Exception e) {
            failed = true;
        }
        if (!failed) {
            Assert.fail();
        }
        failed = false;
        try {
            client.multiFieldQueryLast(mlastQuery);
        } catch (Exception e) {
            failed = true;
        }
        if (!failed) {
            Assert.fail();
        }
    }

}
