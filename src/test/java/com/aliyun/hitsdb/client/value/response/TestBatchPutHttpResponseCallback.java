package com.aliyun.hitsdb.client.value.response;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.hitsdb.client.TSDBConfig;
import com.aliyun.hitsdb.client.callback.AbstractBatchPutCallback;
import com.aliyun.hitsdb.client.callback.BatchPutDetailsCallback;
import com.aliyun.hitsdb.client.callback.BatchPutSummaryCallback;
import com.aliyun.hitsdb.client.callback.http.BatchPutHttpResponseCallback;
import com.aliyun.hitsdb.client.exception.http.HttpServerBadRequestException;
import com.aliyun.hitsdb.client.exception.http.HttpServerNotSupportException;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.http.response.ResultResponse;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.response.batch.DetailsResult;
import com.aliyun.hitsdb.client.value.response.batch.ErrorPoint;
import com.aliyun.hitsdb.client.value.response.batch.SummaryResult;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.Collections;
import java.util.List;

import static org.powermock.api.mockito.PowerMockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BatchPutHttpResponseCallback.class, HttpClient.class})
public class TestBatchPutHttpResponseCallback {
    @BeforeClass
    public static void setup() {
        try {
            // call the static constructor of TSDBClient by force
            // so that the setting of the default double deserialization behavior of fastjson would be applied
            Class.forName("com.aliyun.hitsdb.client.TSDBClient");
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
    }

    @Test
    public void testFailedWithResponse() throws Exception {
        // bad request with BatchPutDetailsCallback
        {
            String content = "{\"success\":6,\"failed\":3,\"errors\":[{\"error\":\"Partial write failed. cause: org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException\",\"datapoint\":{\"fields\":{\"hum\":0.34,\"label\":\"hello, ddd\",\"temp\":123.45},\"metric\":\"basetime.metric\",\"tags\":{\"_id\":\"X00001\"},\"timestamp\":1607961600}},{\"error\":\"Partial write failed. cause: org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException\",\"datapoint\":{\"fields\":{\"hum\":0.35,\"label\":\"hello, eee\",\"temp\":123.50},\"metric\":\"basetime.metric\",\"tags\":{\"_id\":\"X00001\"},\"timestamp\":1607961630}},{\"error\":\"Partial write failed. cause: org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException\",\"datapoint\":{\"fields\":{\"hum\":0.35,\"label\":\"hello, eee\",\"temp\":123.50},\"metric\":\"basetime.metric\",\"tags\":{\"_id\":\"X00001\"},\"timestamp\":1607961630}}]}";
            final JSONObject jsonContent = JSON.parseObject(content);
            final JSONArray jsonErrors = jsonContent.getJSONArray("errors");


            ResultResponse response = new ResultResponse(400, content);
            final HttpServerBadRequestException bdex = new HttpServerBadRequestException(response);

            AbstractBatchPutCallback cb = new BatchPutDetailsCallback() {
                @Override
                public void response(String address, List<Point> points, DetailsResult result) {
                    Assert.fail("response() called");
                }

                @Override
                public void failed(String address, List<Point> input, Exception ex) {
                    Assert.fail("failed() whithout details called");
                }

                @Override
                public void partialFailed(String address, List<Point> points, HttpServerBadRequestException ex, DetailsResult result) {
                    Assert.assertTrue(ex == bdex);
                    Assert.assertEquals(ex.getStatus(), 400);

                    Assert.assertEquals(jsonContent.getLong("success").longValue(), result.getSuccess());
                    Assert.assertEquals(jsonContent.getLong("failed").longValue(), result.getFailed());

                    Assert.assertEquals(jsonErrors.size(), result.getErrors().size());
                }
            };

            callFailedWithResponse(createMultiFieldBatchPutHttpResponseCallback(cb), bdex);
        }

        // bad request with BatchPutDetailsCallback
        {
            String content = "{\"failed\":3,\"success\":0,\"errors\":[{\"error\":\"Invalid timestamp\",\"datapoint\":{\"value\":0.35,\"metric\":\"basetime.metric\",\"tags\":{\"_id\":\"X00001\"},\"timestamp\":0}}]}";
            final JSONObject jsonContent = JSON.parseObject(content);
            final JSONArray jsonErrors = jsonContent.getJSONArray("errors");


            ResultResponse response = new ResultResponse(400, content);
            final HttpServerBadRequestException bdex = new HttpServerBadRequestException(response);

            AbstractBatchPutCallback cb = new BatchPutDetailsCallback() {
                @Override
                public void response(String address, List<Point> points, DetailsResult result) {
                    Assert.fail("response() called");
                }

                @Override
                public void failed(String address, List<Point> input, Exception ex) {
                    Assert.fail("failed() whithout details called");
                }

                @Override
                public void partialFailed(String address, List<Point> points, HttpServerBadRequestException ex, DetailsResult result) {
                    Assert.assertTrue(ex == bdex);
                    Assert.assertEquals(ex.getStatus(), 400);

                    Assert.assertEquals(jsonContent.getLong("success").longValue(), result.getSuccess());
                    Assert.assertEquals(jsonContent.getLong("failed").longValue(), result.getFailed());

                    Assert.assertEquals(jsonErrors.size(), result.getErrors().size());

                    ErrorPoint ep = result.getErrors().get(0);
                    Assert.assertNotNull(ep);
                    Assert.assertTrue("Invalid timestamp".equals(ep.getError()));

                    Assert.assertEquals(0.35, (Double)ep.getDatapoint().getValue(), 0.001);
                    Assert.assertEquals(0L, (long)ep.getDatapoint().getTimestamp());
                    Assert.assertTrue("basetime.metric".equals(ep.getDatapoint().getMetric()));
                }
            };

            callFailedWithResponse(createMultiFieldBatchPutHttpResponseCallback(cb), bdex);
        }

        // bad request with BatchPutDetailsCallback
        // backward compatibility
        {
            String content = "{\"success\":6,\"failed\":3,\"errors\":[{\"error\":\"Partial write failed. cause: org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException\",\"datapoint\":{\"fields\":{\"hum\":0.34,\"label\":\"hello, ddd\",\"temp\":123.45},\"metric\":\"basetime.metric\",\"tags\":{\"_id\":\"X00001\"},\"timestamp\":1607961600}},{\"error\":\"Partial write failed. cause: org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException\",\"datapoint\":{\"fields\":{\"hum\":0.35,\"label\":\"hello, eee\",\"temp\":123.50},\"metric\":\"basetime.metric\",\"tags\":{\"_id\":\"X00001\"},\"timestamp\":1607961630}},{\"error\":\"Partial write failed. cause: org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException\",\"datapoint\":{\"fields\":{\"hum\":0.35,\"label\":\"hello, eee\",\"temp\":123.50},\"metric\":\"basetime.metric\",\"tags\":{\"_id\":\"X00001\"},\"timestamp\":1607961630}}]}";
            final JSONObject jsonContent = JSON.parseObject(content);
            final JSONArray jsonErrors = jsonContent.getJSONArray("errors");


            ResultResponse response = new ResultResponse(400, content);
            final HttpServerBadRequestException bdex = new HttpServerBadRequestException(response);

            AbstractBatchPutCallback cb = new BatchPutDetailsCallback() {
                @Override
                public void response(String address, List<Point> points, DetailsResult result) {
                    Assert.fail("response() called");
                }

                @Override
                public void failed(String address, List<Point> input, Exception ex) {
                    Assert.assertTrue(ex == bdex);
                }
            };

            callFailedWithResponse(createMultiFieldBatchPutHttpResponseCallback(cb), bdex);
        }

        // bad request without details response
        // backward compatibility
        {
            String content = "Invalid timestamp";

            ResultResponse response = new ResultResponse(400, content);
            final HttpServerBadRequestException bdex = new HttpServerBadRequestException(response);

            AbstractBatchPutCallback cb = new BatchPutDetailsCallback() {
                @Override
                public void response(String address, List<Point> points, DetailsResult result) {
                    Assert.fail("response() called");
                }

                @Override
                public void failed(String address, List<Point> input, Exception ex) {
                    Assert.assertTrue(ex == bdex);
                }
            };

            callFailedWithResponse(createMultiFieldBatchPutHttpResponseCallback(cb), bdex);
        }


        // bad request without details response
        {
            String content = "Invalid timestamp";

            ResultResponse response = new ResultResponse(400, content);
            final HttpServerBadRequestException bdex = new HttpServerBadRequestException(response);

            AbstractBatchPutCallback cb = new BatchPutDetailsCallback() {
                @Override
                public void response(String address, List<Point> points, DetailsResult result) {
                    Assert.fail("response() called");
                }

                @Override
                public void failed(String address, List<Point> input, Exception ex) {
                    Assert.fail("failed() whithout details called");
                }

                @Override
                public void partialFailed(String address, List<Point> points, HttpServerBadRequestException ex, DetailsResult result) {
                    Assert.assertNull(result);
                    Assert.assertTrue(ex == bdex);
                }
            };

            callFailedWithResponse(createMultiFieldBatchPutHttpResponseCallback(cb), bdex);
        }


        // bad request with BatchPutSummaryCallback
        {
            String content = "{\"success\":6,\"failed\":3,\"errors\":[{\"error\":\"Partial write failed. cause: org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException\",\"datapoint\":{\"fields\":{\"hum\":0.34,\"label\":\"hello, ddd\",\"temp\":123.45},\"metric\":\"basetime.metric\",\"tags\":{\"_id\":\"X00001\"},\"timestamp\":1607961600}},{\"error\":\"Partial write failed. cause: org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException\",\"datapoint\":{\"fields\":{\"hum\":0.35,\"label\":\"hello, eee\",\"temp\":123.50},\"metric\":\"basetime.metric\",\"tags\":{\"_id\":\"X00001\"},\"timestamp\":1607961630}},{\"error\":\"Partial write failed. cause: org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException\",\"datapoint\":{\"fields\":{\"hum\":0.35,\"label\":\"hello, eee\",\"temp\":123.50},\"metric\":\"basetime.metric\",\"tags\":{\"_id\":\"X00001\"},\"timestamp\":1607961630}}]}";
            final JSONObject jsonContent = JSON.parseObject(content);


            ResultResponse response = new ResultResponse(400, content);
            HttpServerBadRequestException ex = new HttpServerBadRequestException(response);

            AbstractBatchPutCallback cb = new BatchPutSummaryCallback() {
                @Override
                public void response(String address, List<Point> points, SummaryResult result) {
                    Assert.fail("response() called");
                }

                @Override
                public void failed(String address, List<Point> input, Exception ex) {
                    Assert.fail("failed() whithout summary called");
                }

                @Override
                public void partialFailed(String address, List<Point> points, HttpServerBadRequestException ex, SummaryResult result) {
                    Assert.assertEquals(jsonContent.getLong("success").longValue(), result.getSuccess());
                    Assert.assertEquals(jsonContent.getLong("failed").longValue(), result.getFailed());
                }
            };

            callFailedWithResponse(createMultiFieldBatchPutHttpResponseCallback(cb), ex);
        }

        // bad request with BatchPutSummaryCallback
        {
            String content = "{\"success\":6,\"failed\":3}";
            final JSONObject jsonContent = JSON.parseObject(content);
            final JSONArray jsonErrors = jsonContent.getJSONArray("errors");


            ResultResponse response = new ResultResponse(400, content);
            final HttpServerBadRequestException bdex = new HttpServerBadRequestException(response);

            AbstractBatchPutCallback cb = new BatchPutSummaryCallback() {
                @Override
                public void response(String address, List<Point> points, SummaryResult result) {
                    Assert.fail("response() called");
                }

                @Override
                public void failed(String address, List<Point> input, Exception ex) {
                    Assert.fail("failed() whithout summary called");
                }

                @Override
                public void partialFailed(String address, List<Point> points, HttpServerBadRequestException ex, SummaryResult result) {
                    Assert.assertEquals(jsonContent.getLong("success").longValue(), result.getSuccess());
                    Assert.assertEquals(jsonContent.getLong("failed").longValue(), result.getFailed());
                }
            };

            callFailedWithResponse(createMultiFieldBatchPutHttpResponseCallback(cb), bdex);
        }

        // bad request with BatchPutSummaryCallback
        // backward compatibility
        {
            String content = "{\"success\":6,\"failed\":3}";
            final JSONObject jsonContent = JSON.parseObject(content);
            final JSONArray jsonErrors = jsonContent.getJSONArray("errors");


            ResultResponse response = new ResultResponse(400, content);
            final HttpServerBadRequestException bdex = new HttpServerBadRequestException(response);

            AbstractBatchPutCallback cb = new BatchPutSummaryCallback() {
                @Override
                public void response(String address, List<Point> points, SummaryResult result) {
                    Assert.fail("response() called");
                }

                @Override
                public void failed(String address, List<Point> input, Exception ex) {
                    Assert.assertTrue(ex == bdex);
                }
            };

            callFailedWithResponse(createMultiFieldBatchPutHttpResponseCallback(cb), bdex);
        }

        // bad request with BatchPutSummaryCallback
        {
            String content = "{\"success\":6,\"failed\":3}";
            final JSONObject jsonContent = JSON.parseObject(content);
            final JSONArray jsonErrors = jsonContent.getJSONArray("errors");


            ResultResponse response = new ResultResponse(400, content);
            final HttpServerNotSupportException bdex = new HttpServerNotSupportException(response);

            AbstractBatchPutCallback cb = new BatchPutSummaryCallback() {
                @Override
                public void response(String address, List<Point> points, SummaryResult result) {
                    Assert.fail("response() called");
                }

                @Override
                public void failed(String address, List<Point> input, Exception ex) {
                    Assert.assertTrue(ex == bdex);
                }

                @Override
                public void partialFailed(String address, List<Point> points, HttpServerBadRequestException ex, SummaryResult result) {
                    Assert.fail("failed() whith summary called");
                }
            };

            callFailedWithResponse(createMultiFieldBatchPutHttpResponseCallback(cb), bdex);
        }
    }

    private BatchPutHttpResponseCallback createMultiFieldBatchPutHttpResponseCallback(AbstractBatchPutCallback<?> batchPutCallback) {
        TSDBConfig config = TSDBConfig.address("127.0.0.1", 3002).config();
        HttpClient client = mock(HttpClient.class);

        BatchPutHttpResponseCallback callback = new BatchPutHttpResponseCallback("127.0.0.1", client,
                batchPutCallback, Collections.EMPTY_LIST,  config, 3);
        return callback;
    }

    private void callFailedWithResponse(BatchPutHttpResponseCallback callback, Exception ex) throws Exception {
        Whitebox.invokeMethod(callback, "failedWithResponse", ex);
    }
}
