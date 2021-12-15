package com.aliyun.hitsdb.client;

import com.aliyun.hitsdb.client.value.request.*;
import com.aliyun.hitsdb.client.value.response.LastDataValue;
import com.aliyun.hitsdb.client.value.response.MultiFieldQueryLastResult;
import com.aliyun.hitsdb.client.value.response.MultiFieldQueryResult;
import com.aliyun.hitsdb.client.value.response.QueryResult;
import com.aliyun.hitsdb.client.value.type.Aggregator;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by yunxing on 2021/2/5.
 */
public class TestClientQueryHA {
    /**
     * @author yunxing
     * @description Test case for 4 query HA policy : SecondaryPreferred, PrimaryPreferred, Primary and Secondary.
     *              Attention: We must start up 2 cluster before testing, and follow the instruction during testing
     * @throws InterruptedException
     */
    @Test
    public void testHA() throws InterruptedException {
        final String metric = "TestClientQueryHA";
        final String tagName = "_id";
        final String tagValue = "tagv";
        final String mainIp = "127.0.0.1";
        final int mainPort = 8242;
        final String secondaryIp = "127.0.0.1";
        final int secondaryPort = 8243;
        HashMap<String, String> tags = new HashMap<String, String>() {{ put(tagName, tagValue);}};

        TSDBConfig config = TSDBConfig.address(mainIp, mainPort).config();
        TSDBClient tsdb = new TSDBClient(config);

        TSDBConfig config2 = TSDBConfig.address(secondaryIp, secondaryPort).config();
        TSDBClient tsdb2 = new TSDBClient(config2);

        long currentSecond = System.currentTimeMillis() / 1000;
        int value = 1;
        Boolean failed = false;

        Point point = Point.metric(metric).tag(tagName, tagValue).value(currentSecond, value).build();
        MultiFieldPoint mpoint = MultiFieldPoint.metric(metric).tag(tagName, tagValue).timestamp(currentSecond).field("field", value).build();
        tsdb.putSync(point);
        tsdb.multiFieldPutSync(mpoint);
        tsdb2.putSync(point);
        tsdb2.multiFieldPutSync(mpoint);


        // STEP 1: TEST FOR HAPolicy.RetryRule.Secondary AND HAPolicy.RetryRule.SecondaryPreferred
        HAPolicy SecondaryPreferredpolicy = HAPolicy.addSecondaryCluster(secondaryIp, secondaryPort).setReadRule(HAPolicy.ReadRule.SecondaryPreferred).setQueryRetryTimes(1).build();
        TSDBConfig SecondaryPreferredConfig = TSDBConfig.address(mainIp, mainPort).addHAPolicy(SecondaryPreferredpolicy).config();
        TSDBClient secondaryPreferredTsdb = new TSDBClient(SecondaryPreferredConfig);

        HAPolicy secondaryPolicy = HAPolicy.addSecondaryCluster(secondaryIp, secondaryPort).setReadRule(HAPolicy.ReadRule.Secondary).setQueryRetryTimes(1).build();
        TSDBConfig secondaryConfig = TSDBConfig.address(mainIp, mainPort).addHAPolicy(secondaryPolicy).config();
        TSDBClient secondaryTsdb = new TSDBClient(secondaryConfig);

        System.out.println("please kill secondary cluster!");
        Thread.sleep(5 * 1000);
        while (true) {
            try {
                TSDBConfig tempConfig = TSDBConfig.address(secondaryIp, secondaryPort).config();
                new TSDBClient(tempConfig);
            } catch (Exception e) {
                break;
            }
            System.out.println("please kill secondary cluster!");
            Thread.sleep(5 * 1000);
        }

        Query query = Query.timeRange(currentSecond - 0, currentSecond + 1)
                .sub(SubQuery.metric(metric).aggregator(Aggregator.NONE).tag(tags).build()).build();
        List<QueryResult> results = secondaryPreferredTsdb.query(query);
        Assert.assertTrue(results.size() > 0);
        failed = false;
        try {
            secondaryTsdb.query(query);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed);

        MultiFieldQuery mquery = MultiFieldQuery.timeRange(currentSecond - 0, currentSecond + 1)
                .sub(MultiFieldSubQuery.metric(metric).tags(tags)
                    .fieldsInfo(new MultiFieldSubQueryDetails.Builder("*", Aggregator.NONE).build())
                    .build())
                .build();
        List<MultiFieldQueryResult> mresults = secondaryPreferredTsdb.multiFieldQuery(mquery);
        Assert.assertTrue(mresults.size() > 0);
        failed = false;
        try {
            secondaryTsdb.multiFieldQuery(mquery);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed);

        LastPointQuery lastQuery = LastPointQuery
                .builder().backScan(0).msResolution(true)
                .sub(LastPointSubQuery.builder(metric, tags).build()).build();
        List<LastDataValue> lresults = secondaryPreferredTsdb.queryLast(lastQuery);
        Assert.assertTrue(lresults.size() > 0);
        failed = false;
        try {
            secondaryTsdb.queryLast(lastQuery);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed);

        LastPointQuery mlastQuery = LastPointQuery
                .builder().backScan(0).msResolution(true)
                .sub(LastPointSubQuery.builder(metric, new ArrayList<String>() {{add("*");}}, tags).build()).build();
        List<MultiFieldQueryLastResult>  mlresults = secondaryPreferredTsdb.multiFieldQueryLast(mlastQuery);
        Assert.assertTrue(mlresults.size() > 0);
        failed = false;
        try {
            secondaryTsdb.multiFieldQueryLast(mlastQuery);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed);

        // STEP 2: TEST FOR HAPolicy.RetryRule.Primary AND HAPolicy.RetryRule.PrimaryPreferred
        while (true) {
            try {
                TSDBConfig tempConfig = TSDBConfig.address(secondaryIp, secondaryPort).config();
                new TSDBClient(tempConfig);
                break;
            } catch (Exception e) {
                System.out.println("please start up secondary cluster!");
                Thread.sleep(5 * 1000);
            }
        }

        HAPolicy PrimaryPreferredPolicy = HAPolicy.addSecondaryCluster(secondaryIp, secondaryPort).setReadRule(HAPolicy.ReadRule.PrimaryPreferred).setQueryRetryTimes(1).build();
        TSDBConfig PrimaryPreferredConfig = TSDBConfig.address(mainIp, mainPort).addHAPolicy(PrimaryPreferredPolicy).config();
        TSDBClient primaryPreferredTsdb = new TSDBClient(PrimaryPreferredConfig);

        HAPolicy mainPolicy = HAPolicy.addSecondaryCluster(secondaryIp, secondaryPort).setReadRule(HAPolicy.ReadRule.Primary).setQueryRetryTimes(1).build();
        TSDBConfig mainConfig = TSDBConfig.address(mainIp, mainPort).addHAPolicy(mainPolicy).config();
        TSDBClient mainTsdb = new TSDBClient(mainConfig);

        System.out.println("please kill main cluster!");
        Thread.sleep(5 * 1000);
        while (true) {
            try {
                TSDBConfig tempConfig = TSDBConfig.address(mainIp, mainPort).config();
                new TSDBClient(tempConfig);
            } catch (Exception e) {
                break;
            }
            System.out.println("please kill main cluster!");
            Thread.sleep(5 * 1000);
        }

        results = primaryPreferredTsdb.query(query);
        Assert.assertTrue(results.size() > 0);
        failed = false;
        try {
            mainTsdb.query(query);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed);

        mresults = primaryPreferredTsdb.multiFieldQuery(mquery);
        Assert.assertTrue(mresults.size() > 0);
        failed = false;
        try {
            mainTsdb.multiFieldQuery(mquery);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed);

        lresults = primaryPreferredTsdb.queryLast(lastQuery);
        Assert.assertTrue(lresults.size() > 0);
        failed = false;
        try {
            mainTsdb.queryLast(lastQuery);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed);

        mlresults = primaryPreferredTsdb.multiFieldQueryLast(mlastQuery);
        Assert.assertTrue(mlresults.size() > 0);
        failed = false;
        try {
            mainTsdb.multiFieldQueryLast(mlastQuery);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed);
    }

    @Test
    public void testHAtoSameInstance() throws InterruptedException {
        String readHost = "127.0.0.1", writeHost = "127.0.0.1";
        int readPort = 8242, writePort = 8242;
        String writerUsername = "testuser", tsdbWritePassword = "asdf1234";

        HAPolicy policy = HAPolicy.addSecondaryCluster(readHost, readPort)
                .setReadRule(HAPolicy.ReadRule.SecondaryPreferred)
                .setQueryRetryTimes(0)
                .build();

        TSDBConfig config = TSDBConfig
                .address(writeHost, writePort)
                .basicAuth(writerUsername, tsdbWritePassword)
                .httpConnectionLiveTime(1800)
                .batchPutSize(50)
                .batchPutRetryCount(3)
                .multiFieldBatchPutConsumerThreadCount(2)
                .addHAPolicy(policy)
                .config();

        TSDBClient client = new TSDBClient(config);
        long timestamp = 1626926400L;

        String metric = "testHAtoSameInstance";

        List<MultiFieldPoint> points = new ArrayList<MultiFieldPoint>();
        for (int i = 0; i < 10; i++) {
            MultiFieldPoint point = MultiFieldPoint.metric(metric)
                    .field("field1", "stringValue"+i)
                    .field("field2", 10.0 + i)
                    .field("field3", true)
                    .tag("tagkey1", "tagvalue1")
                    .tag("tagkey2", "tagvalue2")
                    .timestamp(timestamp+i).build(true);
            points.add(point);
        }
        client.multiFieldPutSync(points);

        Thread.sleep(2000);

        // Query Data

        MultiFieldQuery query1 = MultiFieldQuery.start(timestamp).end(timestamp+10)
                .sub(MultiFieldSubQuery.metric(metric).fieldsInfo(MultiFieldSubQueryDetails.field("field1").aggregator(Aggregator.NONE).build())
                .build()).msResolution(false).build();

        System.out.println(query1.toJSON());
        List<MultiFieldQueryResult> results = client.multiFieldQuery(query1);
        System.out.println(results.toString());
    }
}
