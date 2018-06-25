package com.aliyun.hitsdb.client;

import java.io.IOException;
import java.util.*;

import com.aliyun.hitsdb.client.value.request.*;
import com.aliyun.hitsdb.client.value.response.LookupDetailedResult;
import com.aliyun.hitsdb.client.value.response.LookupResult;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;

public class TestHiTSDBClientLookup {
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

    @Test
    public void testQueryLookup() throws InterruptedException {

        /** /api/search/lookup Test */
        final String Metric1 = "testLookupWithVariousOptions_1";
        final String Metric2 = "testLookupWithVariousOptions_2";
        final String Tagk1_prefix = "k1.testLookupWithVariousOptions.";
        final String Tagk2_prefix = "k2.testLookupWithVariousOptions.";
        final String Tagv1_prefix = "v1.testLookupWithVariousOptions.";
        final String Tagv2_prefix = "v2.testLookupWithVariousOptions.";

        List<Point> putlist = new ArrayList<Point>();
        Map<String, String> tagkv_1 = new HashMap<String, String>();
        Map<String, String> tagvk_2 = new HashMap<String, String>();
        final int startTime = 1511927288;

        // 生成新的时间线
        for (int i = 0; i < 20; i++) {
            int j = i % 5;
            tagkv_1.put(Tagk1_prefix+i, Tagv1_prefix+j);
            tagvk_2.put(Tagv2_prefix+i, Tagk2_prefix+j);
            Point point = Point.metric(Metric1)
                    .tag(Tagk1_prefix+i, Tagv1_prefix+j)
                    .tag(Tagk2_prefix+j, Tagv2_prefix+i).timestamp(startTime)
                    .value(100.00).build();
            putlist.add(point);
            point = Point.metric(Metric2)
                    .tag(Tagk1_prefix+i, Tagv1_prefix+j)
                    .tag(Tagk2_prefix+j, Tagv2_prefix+i).timestamp(startTime)
                    .value(100.00).build();
            putlist.add(point);
        }
        tsdb.putSync(putlist);
        Thread.sleep(2000);

        // Lookup with Metric Only
        {
            LookupRequest lookup = LookupRequest.metric(Metric1).build();
            System.out.println("查询条件：" + lookup.toJSON());
            List<LookupResult> lookupResults = tsdb.lookup(lookup);
            System.out.println("查询结果：" + lookupResults);

            if (lookupResults.size() != 1) {
                Assert.fail("Incorrect lookup return result. Result set size is: " + lookupResults.size());
            }

            // Return result verification
            List<LookupDetailedResult> results = lookupResults.get(0).getResults();
            if (results.size() != 20) {
                Assert.fail("Incorrect lookup return result. " +
                        "We are expecting 20 time series satisfy the condition. " +
                        "Actual :"+ results.size());
            }

            for (LookupDetailedResult result : results) {
                String result_metric = result.getMetric();
                Map<String, String> result_tags = result.getTags();

                if (!result_metric.equals(Metric1)) {
                    Assert.fail("Lookup result is not right. Lookup Result Metric: "
                            + result_metric + ". Expected Metric: " + Metric1);
                }

                if (result_tags == null || result_tags.isEmpty() || result_tags.size() != 2) {
                    Assert.fail("Lookup result's tags field size incorrect. Size: " + result_tags.size());
                }

                for (Map.Entry<String, String> tag : result_tags.entrySet()) {
                    String tagk = tag.getKey();
                    String tagv = tag.getValue();

                    if (!(tagkv_1.get(tagk) != null && tagkv_1.get(tagk).equals(tagv))
                            && !(tagvk_2.get(tagv) != null && tagvk_2.get(tagv).equals(tagk))) {
                        Assert.fail("Lookup result's tags field is incorrect. " +
                                "Result Tagkv: " + tagk + " : " + tagv);
                    }
                }
            }
        }

        // Lookup with Tagk only
        {
            final String lookup_tagk = "k2.testLookupWithVariousOptions.3";

            LookupRequest lookup = LookupRequest.tags(lookup_tagk, "*").build();
            System.out.println("查询条件：" + lookup.toJSON());
            List<LookupResult> lookupResults = tsdb.lookup(lookup);
            System.out.println("查询结果：" + lookupResults);

            if (lookupResults.size() != 1) {
                Assert.fail("Incorrect lookup return result. Result set size is: " + lookupResults.size());
            }

            // Return result verification
            List<LookupDetailedResult> results = lookupResults.get(0).getResults();
            if (results.size() != 8) {
                Assert.fail("Incorrect lookup return result. " +
                        "We are expecting 8 time series satisfy the condition. " +
                        "Actual :"+ results.size());
            }

            for (LookupDetailedResult result : results) {
                boolean contain_lookup_tagk = false;
                String result_metric = result.getMetric();
                Map<String, String> result_tags = result.getTags();

                if (!result_metric.equals(Metric1) && !result_metric.equals(Metric2)) {
                    Assert.fail("Lookup result is not right. Lookup Result Metric: "
                            + result_metric + ". Expected Metric: " + Metric1 + " or " + Metric2);
                }

                if (result_tags == null || result_tags.isEmpty() || result_tags.size() != 2) {
                    Assert.fail("Lookup result's tags field size incorrect. Size: " + result_tags.size());
                }

                for (Map.Entry<String, String> tag : result_tags.entrySet()) {
                    String tagk = tag.getKey();
                    String tagv = tag.getValue();

                    if (lookup_tagk.equals(tagk)) {
                        contain_lookup_tagk = true;
                    }

                    if (!(tagkv_1.get(tagk) != null && tagkv_1.get(tagk).equals(tagv))
                            && !(tagvk_2.get(tagv) != null && tagvk_2.get(tagv).equals(tagk))) {
                        Assert.fail("Lookup result's tags field is incorrect. " +
                                "Result Tagkv: " + tagk + " : " + tagv);
                    }
                }

                if (!contain_lookup_tagk) {
                    Assert.fail("Lookup result's tags field does not satisfy lookup condition.");
                }
            }
        }

        // Lookup with Tagv only
        {
            final String lookup_tagv = "v1.testLookupWithVariousOptions.2";
            LookupRequest lookup = LookupRequest.tags("*", lookup_tagv).build();
            System.out.println("查询条件：" + lookup.toJSON());
            List<LookupResult> lookupResults = tsdb.lookup(lookup);
            System.out.println("查询结果：" + lookupResults);

            if (lookupResults.size() != 1) {
                Assert.fail("Incorrect lookup return result. Result set size is: " + lookupResults.size());
            }

            // Return result verification
            List<LookupDetailedResult> results = lookupResults.get(0).getResults();
            if (results.size() != 8) {
                Assert.fail("Incorrect lookup return result. " +
                        "We are expecting 8 time series satisfy the condition. " +
                        "Actual :"+ results.size());
            }

            for (LookupDetailedResult result : results) {
                boolean contain_lookup_tagv = false;
                String result_metric = result.getMetric();
                Map<String, String> result_tags = result.getTags();

                if (!result_metric.equals(Metric1) && !result_metric.equals(Metric2)) {
                    Assert.fail("Lookup result is not right. Lookup Result Metric: "
                            + result_metric + ". Expected Metric: " + Metric1 + " or " + Metric2);
                }

                if (result_tags == null || result_tags.isEmpty() || result_tags.size() != 2) {
                    Assert.fail("Lookup result's tags field size incorrect. Size: " + result_tags.size());
                }

                for (Map.Entry<String, String> tag : result_tags.entrySet()) {
                    String tagk = tag.getKey();
                    String tagv = tag.getValue();

                    if (lookup_tagv.equals(tagv)) {
                        contain_lookup_tagv = true;
                    }

                    if (!(tagkv_1.get(tagk) != null && tagkv_1.get(tagk).equals(tagv))
                            && !(tagvk_2.get(tagv) != null && tagvk_2.get(tagv).equals(tagk))) {
                        Assert.fail("Lookup result's tags field is incorrect. " +
                                "Result Tagkv: " + tagk + " : " + tagv);
                    }
                }

                if (!contain_lookup_tagv) {
                    Assert.fail("Lookup result's tags field does not satisfy lookup condition.");
                }
            }
        }

        // Lookup with Tagkv pair only
        {
            final String lookup_tagk = "k1.testLookupWithVariousOptions.7";
            final String lookup_tagv = "v1.testLookupWithVariousOptions.2";

            LookupRequest lookup = LookupRequest.tags(lookup_tagk, lookup_tagv).build();
            System.out.println("查询条件：" + lookup.toJSON());
            List<LookupResult> lookupResults = tsdb.lookup(lookup);
            System.out.println("查询结果：" + lookupResults);

            if (lookupResults.size() != 1) {
                Assert.fail("Incorrect lookup return result. Result set size is: " + lookupResults.size());
            }

            // Return result verification
            List<LookupDetailedResult> results = lookupResults.get(0).getResults();
            if (results.size() != 2) {
                Assert.fail("Incorrect lookup return result. " +
                        "We are expecting 2 time series satisfy the condition. " +
                        "Actual :"+ results.size());
            }

            for (LookupDetailedResult result : results) {
                boolean contain_lookup_tagkv = false;
                String result_metric = result.getMetric();
                Map<String, String> result_tags = result.getTags();

                if (!result_metric.equals(Metric1) && !result_metric.equals(Metric2)) {
                    Assert.fail("Lookup result is not right. Lookup Result Metric: "
                            + result_metric + ". Expected Metric: " + Metric1 + " or " + Metric2);
                }

                if (result_tags == null || result_tags.isEmpty() || result_tags.size() != 2) {
                    Assert.fail("Lookup result's tags field size incorrect. Size: " + result_tags.size());
                }

                for (Map.Entry<String, String> tag : result_tags.entrySet()) {
                    String tagk = tag.getKey();
                    String tagv = tag.getValue();

                    if (lookup_tagv.equals(tagv) && lookup_tagk.equals(tagk)) {
                        contain_lookup_tagkv = true;
                    }

                    if (!(tagkv_1.get(tagk) != null && tagkv_1.get(tagk).equals(tagv))
                            && !(tagvk_2.get(tagv) != null && tagvk_2.get(tagv).equals(tagk))) {
                        Assert.fail("Lookup result's tags field is incorrect. " +
                                "Result Tagkv: " + tagk + " : " + tagv);
                    }
                }

                if (!contain_lookup_tagkv) {
                    Assert.fail("Lookup result's tags field does not satisfy lookup condition.");
                }
            }
        }

        // Lookup with Metric + Tagk
        {
            final String lookup_tagk = "k2.testLookupWithVariousOptions.1";
            LookupRequest lookup = LookupRequest.metric(Metric1).tags(lookup_tagk, "*").build();
            System.out.println("查询条件：" + lookup.toJSON());
            List<LookupResult> lookupResults = tsdb.lookup(lookup);
            System.out.println("查询结果：" + lookupResults);

            if (lookupResults.size() != 1) {
                Assert.fail("Incorrect lookup return result. Result set size is: " + lookupResults.size());
            }

            // Return result verification
            List<LookupDetailedResult> results = lookupResults.get(0).getResults();
            if (results.size() != 4) {
                Assert.fail("Incorrect lookup return result. " +
                        "We are expecting 4 time series satisfy the condition. " +
                        "Actual :"+ results.size());
            }

            for (LookupDetailedResult result : results) {
                boolean contain_lookup_tagk = false;
                String result_metric = result.getMetric();
                Map<String, String> result_tags = result.getTags();

                if (!result_metric.equals(Metric1)) {
                    Assert.fail("Lookup result is not right. Lookup Result Metric: "
                            + result_metric + ". Expected Metric: " + Metric1);
                }

                if (result_tags == null || result_tags.isEmpty() || result_tags.size() != 2) {
                    Assert.fail("Lookup result's tags field size incorrect. Size: " + result_tags.size());
                }

                for (Map.Entry<String, String> tag : result_tags.entrySet()) {
                    String tagk = tag.getKey();
                    String tagv = tag.getValue();

                    if (lookup_tagk.equals(tagk)) {
                        contain_lookup_tagk = true;
                    }

                    if (!(tagkv_1.get(tagk) != null && tagkv_1.get(tagk).equals(tagv))
                            && !(tagvk_2.get(tagv) != null && tagvk_2.get(tagv).equals(tagk))) {
                        Assert.fail("Lookup result's tags field is incorrect. " +
                                "Result Tagkv: " + tagk + " : " + tagv);
                    }
                }

                if (!contain_lookup_tagk) {
                    Assert.fail("Lookup result's tags field does not satisfy lookup condition.");
                }
            }
        }

        // Lookup with Metric + Tagv
        {
            final String lookup_tagv = "v1.testLookupWithVariousOptions.4";
            LookupRequest lookup = LookupRequest.metric(Metric1).tags("*", lookup_tagv).build();
            System.out.println("查询条件：" + lookup.toJSON());
            List<LookupResult> lookupResults = tsdb.lookup(lookup);
            System.out.println("查询结果：" + lookupResults);

            if (lookupResults.size() != 1) {
                Assert.fail("Incorrect lookup return result. Result set size is: " + lookupResults.size());
            }

            // Return result verification
            List<LookupDetailedResult> results = lookupResults.get(0).getResults();
            if (results.size() != 4) {
                Assert.fail("Incorrect lookup return result. " +
                        "We are expecting 4 time series satisfy the condition. " +
                        "Actual :"+ results.size());
            }

            for (LookupDetailedResult result : results) {
                boolean contain_lookup_tagv = false;
                String result_metric = result.getMetric();
                Map<String, String> result_tags = result.getTags();

                if (!result_metric.equals(Metric1)) {
                    Assert.fail("Lookup result is not right. Lookup Result Metric: "
                            + result_metric + ". Expected Metric: " + Metric1);
                }

                if (result_tags == null || result_tags.isEmpty() || result_tags.size() != 2) {
                    Assert.fail("Lookup result's tags field size incorrect. Size: " + result_tags.size());
                }

                for (Map.Entry<String, String> tag : result_tags.entrySet()) {
                    String tagk = tag.getKey();
                    String tagv = tag.getValue();

                    if (lookup_tagv.equals(tagv)) {
                        contain_lookup_tagv = true;
                    }

                    if (!(tagkv_1.get(tagk) != null && tagkv_1.get(tagk).equals(tagv))
                            && !(tagvk_2.get(tagv) != null && tagvk_2.get(tagv).equals(tagk))) {
                        Assert.fail("Lookup result's tags field is incorrect. " +
                                "Result Tagkv: " + tagk + " : " + tagv);
                    }
                }

                if (!contain_lookup_tagv) {
                    Assert.fail("Lookup result's tags field does not satisfy lookup condition.");
                }
            }
        }

        // Lookup with Metric + Tagvk pair
        {
            final String tagk1 = "k1.testLookupWithVariousOptions.5";
            final String tagv1 = "v1.testLookupWithVariousOptions.0";
            final String tagk2 = "k2.testLookupWithVariousOptions.0";
            final String tagv2 = "v2.testLookupWithVariousOptions.5";

            LookupRequest lookup = LookupRequest.metric(Metric1).tags(tagk2, tagv2).build();
            System.out.println("查询条件：" + lookup.toJSON());
            List<LookupResult> lookupResults = tsdb.lookup(lookup);
            System.out.println("查询结果：" + lookupResults);

            if (lookupResults.size() != 1) {
                Assert.fail("Incorrect lookup return result. Result set size is: " + lookupResults.size());
            }

            // Return result verification
            List<LookupDetailedResult> results = lookupResults.get(0).getResults();
            if (results.size() != 1) {
                Assert.fail("Incorrect lookup return result. " +
                        "We are expecting 1 time series satisfy the condition. " +
                        "Actual :"+ results.size());
            }

            for (LookupDetailedResult result : results) {
                String result_metric = result.getMetric();
                Map<String, String> result_tags = result.getTags();

                if (!result_metric.equals(Metric1)) {
                    Assert.fail("Lookup result is not right. Lookup Result Metric: "
                            + result_metric + ". Expected Metric: " + Metric1);
                }

                if (result_tags == null || result_tags.isEmpty() || result_tags.size() != 2) {
                    Assert.fail("Lookup result's tags field is incorrect.");
                }

                for (Map.Entry<String, String> tag : result_tags.entrySet()) {
                    String result_tagk = tag.getKey();
                    String result_tagv = tag.getValue();

                    if (!(result_tagk.equals(tagk1) && result_tagv.equals(tagv1))
                            && !(result_tagk.equals(tagk2) && result_tagv.equals(tagv2))) {
                        Assert.fail("Lookup result is not right. Tagkv values do not match lookup condition.");
                    }
                }
            }
        }

        // Lookup with Metric + Tagvk pairs
        {
            final String tagv1 = "v1.testLookupWithVariousOptions.0";
            final String tagk2 = "k2.testLookupWithVariousOptions.0";

            LookupRequest lookup = LookupRequest.metric(Metric1)
                    .tags(tagk2, "*").tags("*", tagv1).build();
            System.out.println("查询条件：" + lookup.toJSON());
            List<LookupResult> lookupResults = tsdb.lookup(lookup);
            System.out.println("查询结果：" + lookupResults);

            if (lookupResults.size() != 1) {
                Assert.fail("Incorrect lookup return result. Result set size is: " + lookupResults.size());
            }

            // Return result verification
            List<LookupDetailedResult> results = lookupResults.get(0).getResults();
            if (results.size() != 4) {
                Assert.fail("Incorrect lookup return result. " +
                        "We are expecting 4 time series satisfy the condition. " +
                        "Actual :"+ results.size());
            }

            for (LookupDetailedResult result : results) {
                String result_metric = result.getMetric();
                Map<String, String> result_tags = result.getTags();

                if (!result_metric.equals(Metric1)) {
                    Assert.fail("Lookup result is not right. Lookup Result Metric: "
                            + result_metric + ". Expected Metric: " + Metric1);
                }

                if (result_tags == null || result_tags.isEmpty() || result_tags.size() != 2) {
                    Assert.fail("Lookup result's tags field is incorrect.");
                }

                for (Map.Entry<String, String> tag : result_tags.entrySet()) {
                    String result_tagk = tag.getKey();
                    String result_tagv = tag.getValue();


                    if (result_tagk.equals(tagk2)) {
                        if (!result_tagk.equals(tagvk_2.get(result_tagv))) {
                            Assert.fail("Lookup result is not right. " +
                                    "Tagkv values do not match lookup condition. Result Tagk: "
                                    + result_tagk + ". Tagv:" + result_tagv);
                        }
                    } else if (result_tagv.equals(tagv1)) {
                        if (!result_tagv.equals(tagkv_1.get(result_tagk))) {
                            Assert.fail("Lookup result is not right. " +
                                    "Tagkv values do not match lookup condition. Result Tagk: "
                                    + result_tagk + ". Tagv:" + result_tagv);
                        }
                    } else {
                        Assert.fail("Lookup result is not right. " +
                                "Tagkv values do not match lookup condition. Result Tagk: "
                                + result_tagk + ". Tagv:" + result_tagv);
                    }
                }
            }
        }

        // Make lookup condition impossible to satisfy and confirm result is empty.
        {
            final String tagk = "k2.testLookupWithVariousOptions.4";
            final String tagv = "v2.testLookupWithVariousOptions.10";
            LookupRequest lookup = LookupRequest.metric(Metric1).tags(tagk, tagv).build();
            System.out.println("查询条件：" + lookup.toJSON());
            List<LookupResult> lookupResults = tsdb.lookup(lookup);
            System.out.println("查询结果：" + lookupResults);

            if (lookupResults.size() != 1) {
                Assert.fail("Incorrect lookup return result. Result set size is: " + lookupResults.size());
            }

            // Return result verification
            List<LookupDetailedResult> results = lookupResults.get(0).getResults();
            if (results.size() != 0) {
                Assert.fail("Incorrect lookup return result. " +
                        "We are expecting 0 time series satisfy the condition. " +
                        "Actual :"+ results.size());
            }
        }

        // Metric = metric_name Tagk = *, Tagv = *. Find all time series associated with this metric.
        // This should equivalent to lookup with metric only.
        {
            LookupRequest lookup = LookupRequest.metric(Metric1).tags("*", "*").build();
            System.out.println("查询条件：" + lookup.toJSON());
            List<LookupResult> lookupResults = tsdb.lookup(lookup);
            System.out.println("查询结果：" + lookupResults);

            if (lookupResults.size() != 1) {
                Assert.fail("Incorrect lookup return result. Result set size is: " + lookupResults.size());
            }

            // Return result verification
            List<LookupDetailedResult> results = lookupResults.get(0).getResults();
            if (results.size() != 20) {
                Assert.fail("Incorrect lookup return result. " +
                        "We are expecting 20 time series satisfy the condition. " +
                        "Actual :"+ results.size());
            }

            for (LookupDetailedResult result : results) {
                String result_metric = result.getMetric();
                Map<String, String> result_tags = result.getTags();

                if (!result_metric.equals(Metric1)) {
                    Assert.fail("Lookup result is not right. Lookup Result Metric: "
                            + result_metric + ". Expected Metric: " + Metric1);
                }

                if (result_tags == null || result_tags.isEmpty() || result_tags.size() != 2) {
                    Assert.fail("Lookup result's tags field is incorrect.");
                }

                for (Map.Entry<String, String> tag : result_tags.entrySet()) {
                    String result_tagk = tag.getKey();
                    String result_tagv = tag.getValue();

                    if (!(tagkv_1.get(result_tagk) != null && tagkv_1.get(result_tagk).equals(result_tagv))
                            && !(tagvk_2.get(result_tagv) != null && tagvk_2.get(result_tagv).equals(result_tagk))) {
                        Assert.fail("Lookup result's tags field is incorrect. " +
                                "Result Tagkv: " + result_tagk + " : " + result_tagv);
                    }
                }
            }
        }

        // Metric = *, Tagk = *, Tagv = *. Find all time series inside HiTSDB.
        // Here, we only test limit function. Hence, we do not verify results.
        {
            LookupRequest lookup = LookupRequest.metric("*").tags("*", "*").limit(30).build();
            System.out.println("查询条件：" + lookup.toJSON());
            List<LookupResult> lookupResults = tsdb.lookup(lookup);
            System.out.println("查询结果：" + lookupResults);

            if (lookupResults.size() != 1) {
                Assert.fail("Incorrect lookup return result. Result set size is: " + lookupResults.size());
            }

            // Return result verification
            List<LookupDetailedResult> results = lookupResults.get(0).getResults();
            if (results.size() != 30) {
                Assert.fail("Incorrect lookup return result. " +
                        "We are expecting 30 time series satisfy the condition. " +
                        "Actual :"+ results.size());
            }
        }
    }
}
