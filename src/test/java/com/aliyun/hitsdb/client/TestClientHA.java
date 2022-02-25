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
 * Created by yunxing on 2022/2/25.
 */
public class TestClientHA {
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
        int checkInterval = 5;
        int checkCount = 1;
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
        HAPolicy SecondaryPreferredpolicy = HAPolicy.addSecondaryCluster(secondaryIp, secondaryPort)
                .setRetryRule(HAPolicy.RetryRule.SecondaryPreferred).setRetryTimes(1).setHASwitch(checkCount, checkInterval).build();
        TSDBConfig SecondaryPreferredConfig = TSDBConfig.address(mainIp, mainPort).addHAPolicy(SecondaryPreferredpolicy).config();
        TSDBClient secondaryPreferredTsdb = new TSDBClient(SecondaryPreferredConfig);

        HAPolicy secondaryPolicy = HAPolicy.addSecondaryCluster(secondaryIp, secondaryPort)
                .setRetryRule(HAPolicy.RetryRule.Secondary).setRetryTimes(1).setHASwitch(checkCount, checkInterval).build();
        TSDBConfig secondaryConfig = TSDBConfig.address(mainIp, mainPort).addHAPolicy(secondaryPolicy).config();
        TSDBClient secondaryTsdb = new TSDBClient(secondaryConfig);

        System.out.println("please kill primary cluster!");
        while (true) {
            try {
                TSDBConfig tempConfig = TSDBConfig.address(mainIp, mainPort).config();
                new TSDBClient(tempConfig);
            } catch (Exception e) {
                break;
            }
            System.out.println("please kill primary cluster!");
            Thread.sleep(3 * 1000);
        }


        Thread.sleep(checkCount * checkInterval * 2 * 1000 + 1000);
        // write should recover now
        secondaryPreferredTsdb.putSync(point);
        secondaryTsdb.putSync(point);

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
                TSDBConfig tempConfig = TSDBConfig.address(mainIp, mainPort).config();
                new TSDBClient(tempConfig);
                break;
            } catch (Exception e) {
                System.out.println("please start up primary cluster!");
                Thread.sleep(5 * 1000);
            }
        }

        HAPolicy PrimaryPreferredPolicy = HAPolicy.addSecondaryCluster(secondaryIp, secondaryPort)
                .setRetryRule(HAPolicy.RetryRule.PrimaryPreferred).setRetryTimes(1).setHASwitch(checkCount, checkInterval).build();
        TSDBConfig PrimaryPreferredConfig = TSDBConfig.address(mainIp, mainPort).addHAPolicy(PrimaryPreferredPolicy).config();
        TSDBClient primaryPreferredTsdb = new TSDBClient(PrimaryPreferredConfig);

        HAPolicy mainPolicy = HAPolicy.addSecondaryCluster(secondaryIp, secondaryPort)
                .setRetryRule(HAPolicy.RetryRule.Primary).setRetryTimes(1).setHASwitch(checkCount, checkInterval).build();
        TSDBConfig mainConfig = TSDBConfig.address(mainIp, mainPort).addHAPolicy(mainPolicy).config();
        TSDBClient mainTsdb = new TSDBClient(mainConfig);

        System.out.println("please kill primary cluster again!");
        Thread.sleep(5 * 1000);
        while (true) {
            try {
                TSDBConfig tempConfig = TSDBConfig.address(mainIp, mainPort).config();
                new TSDBClient(tempConfig);
            } catch (Exception e) {
                break;
            }
            System.out.println("please kill primary cluster again!");
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
        int checkInterval = 5;
        int checkCount = 1;

        long currentSecond = System.currentTimeMillis() / 1000;
        int value = 1;
        MultiFieldPoint mpoint = MultiFieldPoint.metric(metric).tag(tagName, tagValue).timestamp(currentSecond).field("field", value).build();


        // STEP 1: TEST FOR HAPolicy.RetryRule.Secondary AND HAPolicy.RetryRule.SecondaryPreferred
        HAPolicy SecondaryPreferredpolicy = HAPolicy.addSecondaryCluster(secondaryIp, secondaryPort)
                .setRetryRule(HAPolicy.RetryRule.SecondaryPreferred).setRetryTimes(1).setHASwitch(checkCount, checkInterval).build();
        TSDBConfig SecondaryPreferredConfig = TSDBConfig.address(mainIp, mainPort).addHAPolicy(SecondaryPreferredpolicy).config();
        TSDBClient secondaryPreferredTsdb = new TSDBClient(SecondaryPreferredConfig);

        while (true) {
            try {
                secondaryPreferredTsdb.multiFieldPutSync(mpoint);
                System.out.println("write success");
            } catch (Exception e) {
                System.out.println("write failed");
            }
            Thread.sleep(1000);
        }
    }
}
