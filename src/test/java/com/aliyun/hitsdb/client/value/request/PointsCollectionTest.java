package com.aliyun.hitsdb.client.value.request;

import com.aliyun.hitsdb.client.callback.BatchPutSummaryCallback;
import com.aliyun.hitsdb.client.callback.MultiFieldBatchPutDetailsCallback;
import com.aliyun.hitsdb.client.value.response.batch.MultiFieldDetailsResult;
import com.aliyun.hitsdb.client.value.response.batch.SummaryResult;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class PointsCollectionTest {
    @Test
    public void createNormally() {
        {
            Set<Point> points = new LinkedHashSet<Point>() {{
                long timestamp = 1608480000L;
                add(Point.metric("createNormally").tag("tag1", "tagv1").timestamp(timestamp).value(12.34).build());
                add(Point.metric("createNormally").tag("tag1", "tagv1").timestamp(timestamp + 30).value(43.21).build());
            }};

            BatchPutSummaryCallback pcb = new BatchPutSummaryCallback() {

                @Override
                public void response(String address, List<Point> input, SummaryResult result) {
                    int success = result.getSuccess();
                    int failed = result.getFailed();
                    System.out.println("write data successfully, success: " + success + ", failure: " + failed);
                }

                @Override
                public void failed(String address, List<Point> input, Exception ex) {
                    System.out.println("failed to write points, " + ex.getMessage());
                }

            };

            PointsCollection pc = new PointsCollection(points, pcb);

            Assert.assertEquals(pc.size(), points.size());
            Assert.assertTrue(pc.toJSON(), pc.toJSON().equals("[{\"metric\":\"createNormally\",\"tags\":{\"tag1\":\"tagv1\"},\"timestamp\":1608480000,\"value\":12.34},{\"metric\":\"createNormally\",\"tags\":{\"tag1\":\"tagv1\"},\"timestamp\":1608480030,\"value\":43.21}]"));
            Assert.assertEquals(pc.asSingleFieldPoints().size(), points.size());
        }


        {
            final long timestamp = 1608480000L;
            List<MultiFieldPoint> points = new ArrayList<MultiFieldPoint>() {{
                add(MultiFieldPoint.metric("createNormally").tag("tag1", "tagv1").timestamp(timestamp).field("f1", 12.34).build());
                add(MultiFieldPoint.metric("createNormally").tag("tag1", "tagv1").timestamp(timestamp + 30).field("f1", 43.21).build());
            }};

            MultiFieldBatchPutDetailsCallback pcb = new MultiFieldBatchPutDetailsCallback() {

                @Override
                public void response(String address, List<MultiFieldPoint> input, MultiFieldDetailsResult result) {
                    int success = result.getSuccess();
                    int failed = result.getFailed();
                    System.out.println("write data successfully, success: " + success + ", failure: " + failed);
                }

                @Override
                public void failed(String address, List<MultiFieldPoint> input, Exception ex) {
                    System.out.println("failed to write points, " + ex.getMessage());
                }

            };

            PointsCollection pc = new PointsCollection(points, pcb);

            Assert.assertEquals(pc.size(), points.size());
            Assert.assertTrue(pc.toJSON(), pc.toJSON().equals("[{\"fields\":{\"f1\":12.34},\"metric\":\"createNormally\",\"tags\":{\"tag1\":\"tagv1\"},\"timestamp\":1608480000},{\"fields\":{\"f1\":43.21},\"metric\":\"createNormally\",\"tags\":{\"tag1\":\"tagv1\"},\"timestamp\":1608480030}]"));
            Assert.assertEquals(pc.asMultiFieldPoints().size(), points.size());
        }
    }

    @Test
    public void createAbnormally() {
        {
            Set<Point> points = new LinkedHashSet<Point>() {{
                long timestamp = 1608480000L;
                add(Point.metric("createNormally").tag("tag1", "tagv1").timestamp(timestamp).value(12.34).build());
                add(Point.metric("createNormally").tag("tag1", "tagv1").timestamp(timestamp + 30).value(43.21).build());
            }};

            try {
                PointsCollection pc = new PointsCollection(points, null);
                Assert.fail("constructor should not succeed");
            } catch (NullPointerException npe) {
                ;;
            }
        }

        {
            final long timestamp = 1608480000L;

            MultiFieldBatchPutDetailsCallback pcb = new MultiFieldBatchPutDetailsCallback() {

                @Override
                public void response(String address, List<MultiFieldPoint> input, MultiFieldDetailsResult result) {
                    int success = result.getSuccess();
                    int failed = result.getFailed();
                    System.out.println("write data successfully, success: " + success + ", failure: " + failed);
                }

                @Override
                public void failed(String address, List<MultiFieldPoint> input, Exception ex) {
                    System.out.println("failed to write points, " + ex.getMessage());
                }

            };

            try {
                PointsCollection pc = new PointsCollection(null, pcb);
                Assert.fail("constructor should not succeed");
            } catch (NullPointerException npe) {
                ;;
            }
        }

        {
            Set<Point> points = new LinkedHashSet<Point>() {{
                long timestamp = 1608480000L;
                add(Point.metric("createNormally").tag("tag1", "tagv1").timestamp(timestamp).value(12.34).build());
                add(Point.metric("createNormally").tag("tag1", "tagv1").timestamp(timestamp + 30).value(43.21).build());
            }};

            BatchPutSummaryCallback pcb = new BatchPutSummaryCallback() {

                @Override
                public void response(String address, List<Point> input, SummaryResult result) {
                    int success = result.getSuccess();
                    int failed = result.getFailed();
                    System.out.println("write data successfully, success: " + success + ", failure: " + failed);
                }

                @Override
                public void failed(String address, List<Point> input, Exception ex) {
                    System.out.println("failed to write points, " + ex.getMessage());
                }

            };

            PointsCollection pc = new PointsCollection(points, pcb);
            pc.add(MultiFieldPoint.metric("createNormally").tag("tag1", "tagv1").timestamp(1608480000L + 60).field("f1", 43.21).build());

            try {
                pc.asSingleFieldPoints();
                Assert.fail("asSingleFieldPoints() should not succeed");
            } catch (IllegalStateException isex) {
                ;;
            }

            try {
                pc.asMultiFieldPoints();
                Assert.fail("asMultiFieldPoints() should not succeed");
            } catch (IllegalStateException isex) {
                ;;
            }
        }
    }
}
