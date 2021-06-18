package com.aliyun.hitsdb.client.value.response;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.hitsdb.client.TSDBConfig;
import com.aliyun.hitsdb.client.callback.AbstractMultiFieldBatchPutCallback;
import com.aliyun.hitsdb.client.callback.MultiFieldBatchPutDetailsCallback;
import com.aliyun.hitsdb.client.callback.MultiFieldBatchPutSummaryCallback;
import com.aliyun.hitsdb.client.callback.http.MultiFieldBatchPutHttpResponseCallback;
import com.aliyun.hitsdb.client.exception.http.HttpServerBadRequestException;
import com.aliyun.hitsdb.client.exception.http.HttpServerNotSupportException;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.http.response.ResultResponse;
import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;
import com.aliyun.hitsdb.client.value.response.batch.MultiFieldDetailsResult;
import com.aliyun.hitsdb.client.value.response.batch.MultiFieldErrorPoint;
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
import java.util.Map;

import static org.powermock.api.mockito.PowerMockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest({MultiFieldBatchPutHttpResponseCallback.class, HttpClient.class})
public class TestMultiFieldBatchPutHttpResponseCallback {

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
        // bad request with MultiFieldBatchPutDetailsCallback
        {
            String content = "{\"success\":6,\"failed\":3,\"errors\":[{\"error\":\"Partial write failed. cause: org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException\",\"datapoint\":{\"fields\":{\"hum\":0.34,\"label\":\"hello, ddd\",\"temp\":123.45},\"metric\":\"basetime.metric\",\"tags\":{\"_id\":\"X00001\"},\"timestamp\":1607961600}},{\"error\":\"Partial write failed. cause: org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException\",\"datapoint\":{\"fields\":{\"hum\":0.35,\"label\":\"hello, eee\",\"temp\":123.50},\"metric\":\"basetime.metric\",\"tags\":{\"_id\":\"X00001\"},\"timestamp\":1607961630}},{\"error\":\"Partial write failed. cause: org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException\",\"datapoint\":{\"fields\":{\"hum\":0.35,\"label\":\"hello, eee\",\"temp\":123.50},\"metric\":\"basetime.metric\",\"tags\":{\"_id\":\"X00001\"},\"timestamp\":1607961630}}]}";
            final JSONObject jsonContent = JSON.parseObject(content);
            final JSONArray jsonErrors = jsonContent.getJSONArray("errors");


            ResultResponse response = new ResultResponse(400, content);
            final HttpServerBadRequestException bdex = new HttpServerBadRequestException(response);

            AbstractMultiFieldBatchPutCallback cb = new MultiFieldBatchPutDetailsCallback() {
                @Override
                public void response(String address, List<MultiFieldPoint> points, MultiFieldDetailsResult result) {
                    Assert.fail("response() called");
                }

                @Override
                public void failed(String address, List<MultiFieldPoint> input, Exception ex) {
                    Assert.fail("failed() whithout details called");
                }

                @Override
                public void partialFailed(String address, List<MultiFieldPoint> points, HttpServerBadRequestException ex, MultiFieldDetailsResult result) {
                    Assert.assertTrue(ex == bdex);
                    Assert.assertEquals(ex.getStatus(), 400);

                    Assert.assertEquals(jsonContent.getLong("success").longValue(), result.getSuccess());
                    Assert.assertEquals(jsonContent.getLong("failed").longValue(), result.getFailed());

                    Assert.assertEquals(jsonErrors.size(), result.getErrors().size());

                    for (MultiFieldErrorPoint ep : result.getErrors()) {
                        Assert.assertNotNull(ep.getDatapoint());
                        Assert.assertEquals(3, ep.getDatapoint().getFields().size());
                    }
                }
            };

            callFailedWithResponse(createMultiFieldBatchPutHttpResponseCallback(cb), bdex);
        }

        // bad request with MultiFieldBatchPutDetailsCallback
        {
            String content = "{\"failed\":3,\"success\":0,\"errors\":[{\"error\":\"Invalid timestamp\",\"datapoint\":{\"fields\":{\"hum\":0.35,\"label\":\"hello, eee\",\"temp\":123.50},\"metric\":\"basetime.metric\",\"tags\":{\"_id\":\"X00001\"},\"timestamp\":0}}]}";
            final JSONObject jsonContent = JSON.parseObject(content);
            final JSONArray jsonErrors = jsonContent.getJSONArray("errors");


            ResultResponse response = new ResultResponse(400, content);
            final HttpServerBadRequestException bdex = new HttpServerBadRequestException(response);

            AbstractMultiFieldBatchPutCallback cb = new MultiFieldBatchPutDetailsCallback() {
                @Override
                public void response(String address, List<MultiFieldPoint> points, MultiFieldDetailsResult result) {
                    Assert.fail("response() called");
                }

                @Override
                public void failed(String address, List<MultiFieldPoint> input, Exception ex) {
                    Assert.fail("failed() whithout details called");
                }

                @Override
                public void partialFailed(String address, List<MultiFieldPoint> points, HttpServerBadRequestException ex, MultiFieldDetailsResult result) {
                    Assert.assertTrue(ex == bdex);
                    Assert.assertEquals(ex.getStatus(), 400);

                    Assert.assertEquals(jsonContent.getLong("success").longValue(), result.getSuccess());
                    Assert.assertEquals(jsonContent.getLong("failed").longValue(), result.getFailed());

                    Assert.assertEquals(jsonErrors.size(), result.getErrors().size());

                    MultiFieldErrorPoint ep = result.getErrors().get(0);
                    Assert.assertNotNull(ep);
                    Assert.assertTrue("Invalid timestamp".equals(ep.getError()));
                    Map<String, Object> fields = ep.getDatapoint().getFields();
                    Assert.assertEquals(0.35, (Double)fields.get("hum"), 0.001);
                    Assert.assertEquals(123.5, (Double)fields.get("temp"), 0.001);
                    Assert.assertTrue("hello, eee".equals(fields.get("label")));
                    Assert.assertEquals(0L, (long)ep.getDatapoint().getTimestamp());
                    Assert.assertTrue("basetime.metric".equals(ep.getDatapoint().getMetric()));

                }
            };

            callFailedWithResponse(createMultiFieldBatchPutHttpResponseCallback(cb), bdex);
        }

        // bad request with MultiFieldBatchPutDetailsCallback
        // backward compatibility
        {
            String content = "{\"success\":6,\"failed\":3,\"errors\":[{\"error\":\"Partial write failed. cause: org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException\",\"datapoint\":{\"fields\":{\"hum\":0.34,\"label\":\"hello, ddd\",\"temp\":123.45},\"metric\":\"basetime.metric\",\"tags\":{\"_id\":\"X00001\"},\"timestamp\":1607961600}},{\"error\":\"Partial write failed. cause: org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException\",\"datapoint\":{\"fields\":{\"hum\":0.35,\"label\":\"hello, eee\",\"temp\":123.50},\"metric\":\"basetime.metric\",\"tags\":{\"_id\":\"X00001\"},\"timestamp\":1607961630}},{\"error\":\"Partial write failed. cause: org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException\",\"datapoint\":{\"fields\":{\"hum\":0.35,\"label\":\"hello, eee\",\"temp\":123.50},\"metric\":\"basetime.metric\",\"tags\":{\"_id\":\"X00001\"},\"timestamp\":1607961630}}]}";
            final JSONObject jsonContent = JSON.parseObject(content);
            final JSONArray jsonErrors = jsonContent.getJSONArray("errors");


            ResultResponse response = new ResultResponse(400, content);
            final HttpServerBadRequestException bdex = new HttpServerBadRequestException(response);

            AbstractMultiFieldBatchPutCallback cb = new MultiFieldBatchPutDetailsCallback() {
                @Override
                public void response(String address, List<MultiFieldPoint> points, MultiFieldDetailsResult result) {
                    Assert.fail("response() called");
                }

                @Override
                public void failed(String address, List<MultiFieldPoint> input, Exception ex) {
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

            AbstractMultiFieldBatchPutCallback cb = new MultiFieldBatchPutDetailsCallback() {
                @Override
                public void response(String address, List<MultiFieldPoint> points, MultiFieldDetailsResult result) {
                    Assert.fail("response() called");
                }

                @Override
                public void failed(String address, List<MultiFieldPoint> input, Exception ex) {
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

            AbstractMultiFieldBatchPutCallback cb = new MultiFieldBatchPutDetailsCallback() {
                @Override
                public void response(String address, List<MultiFieldPoint> points, MultiFieldDetailsResult result) {
                    Assert.fail("response() called");
                }

                @Override
                public void partialFailed(String address, List<MultiFieldPoint> points, HttpServerBadRequestException ex, MultiFieldDetailsResult result) {
                    Assert.fail("partialFailed() called");
                }
            };

            callFailedWithResponse(createMultiFieldBatchPutHttpResponseCallback(cb), bdex);
        }


        // bad request without details response
        {
            String content = "Invalid timestamp";

            ResultResponse response = new ResultResponse(400, content);
            final HttpServerBadRequestException bdex = new HttpServerBadRequestException(response);

            AbstractMultiFieldBatchPutCallback cb = new MultiFieldBatchPutDetailsCallback() {
                @Override
                public void response(String address, List<MultiFieldPoint> points, MultiFieldDetailsResult result) {
                    Assert.fail("response() called");
                }

                @Override
                public void failed(String address, List<MultiFieldPoint> input, Exception ex) {
                    Assert.assertTrue(ex == bdex);
                }

                @Override
                public void partialFailed(String address, List<MultiFieldPoint> points, HttpServerBadRequestException ex, MultiFieldDetailsResult result) {
                    Assert.fail("partialFailed() with details called");
                }
            };

            callFailedWithResponse(createMultiFieldBatchPutHttpResponseCallback(cb), bdex);
        }


        // bad request with MultiFieldBatchPutSummaryCallback
        {
            String content = "{\"success\":6,\"failed\":3,\"errors\":[{\"error\":\"Partial write failed. cause: org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException\",\"datapoint\":{\"fields\":{\"hum\":0.34,\"label\":\"hello, ddd\",\"temp\":123.45},\"metric\":\"basetime.metric\",\"tags\":{\"_id\":\"X00001\"},\"timestamp\":1607961600}},{\"error\":\"Partial write failed. cause: org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException\",\"datapoint\":{\"fields\":{\"hum\":0.35,\"label\":\"hello, eee\",\"temp\":123.50},\"metric\":\"basetime.metric\",\"tags\":{\"_id\":\"X00001\"},\"timestamp\":1607961630}},{\"error\":\"Partial write failed. cause: org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException\",\"datapoint\":{\"fields\":{\"hum\":0.35,\"label\":\"hello, eee\",\"temp\":123.50},\"metric\":\"basetime.metric\",\"tags\":{\"_id\":\"X00001\"},\"timestamp\":1607961630}}]}";
            final JSONObject jsonContent = JSON.parseObject(content);


            ResultResponse response = new ResultResponse(400, content);
            HttpServerBadRequestException ex = new HttpServerBadRequestException(response);

            AbstractMultiFieldBatchPutCallback cb = new MultiFieldBatchPutSummaryCallback() {
                @Override
                public void response(String address, List<MultiFieldPoint> points, SummaryResult result) {
                    Assert.fail("response() called");
                }

                @Override
                public void failed(String address, List<MultiFieldPoint> input, Exception ex) {
                    Assert.fail("failed() whithout summary called");
                }

                @Override
                public void partialFailed(String address, List<MultiFieldPoint> points, HttpServerBadRequestException ex, SummaryResult result) {
                    Assert.assertEquals(jsonContent.getLong("success").longValue(), result.getSuccess());
                    Assert.assertEquals(jsonContent.getLong("failed").longValue(), result.getFailed());
                }
            };

            callFailedWithResponse(createMultiFieldBatchPutHttpResponseCallback(cb), ex);
        }

        // bad request with MultiFieldBatchPutSummaryCallback
        {
            String content = "{\"success\":6,\"failed\":3}";
            final JSONObject jsonContent = JSON.parseObject(content);
            final JSONArray jsonErrors = jsonContent.getJSONArray("errors");


            ResultResponse response = new ResultResponse(400, content);
            final HttpServerBadRequestException bdex = new HttpServerBadRequestException(response);

            AbstractMultiFieldBatchPutCallback cb = new MultiFieldBatchPutSummaryCallback() {
                @Override
                public void response(String address, List<MultiFieldPoint> points, SummaryResult result) {
                    Assert.fail("response() called");
                }

                @Override
                public void failed(String address, List<MultiFieldPoint> input, Exception ex) {
                    Assert.fail("failed() whithout summary called");
                }

                @Override
                public void partialFailed(String address, List<MultiFieldPoint> points, HttpServerBadRequestException ex, SummaryResult result) {
                    Assert.assertEquals(jsonContent.getLong("success").longValue(), result.getSuccess());
                    Assert.assertEquals(jsonContent.getLong("failed").longValue(), result.getFailed());
                }
            };

            callFailedWithResponse(createMultiFieldBatchPutHttpResponseCallback(cb), bdex);
        }

        // bad request with MultiFieldBatchPutSummaryCallback
        // backward compatibility
        {
            String content = "{\"success\":6,\"failed\":3}";
            final JSONObject jsonContent = JSON.parseObject(content);
            final JSONArray jsonErrors = jsonContent.getJSONArray("errors");


            ResultResponse response = new ResultResponse(400, content);
            final HttpServerBadRequestException bdex = new HttpServerBadRequestException(response);

            AbstractMultiFieldBatchPutCallback cb = new MultiFieldBatchPutSummaryCallback() {
                @Override
                public void response(String address, List<MultiFieldPoint> points, SummaryResult result) {
                    Assert.fail("response() called");
                }

                @Override
                public void failed(String address, List<MultiFieldPoint> input, Exception ex) {
                    Assert.assertTrue(ex == bdex);
                }
            };

            callFailedWithResponse(createMultiFieldBatchPutHttpResponseCallback(cb), bdex);
        }

        // bad request with MultiFieldBatchPutSummaryCallback
        {
            String content = "Invalid timestamp";

            ResultResponse response = new ResultResponse(400, content);
            final HttpServerBadRequestException bdex = new HttpServerBadRequestException(response);

            AbstractMultiFieldBatchPutCallback cb = new MultiFieldBatchPutSummaryCallback() {
                @Override
                public void response(String address, List<MultiFieldPoint> points, SummaryResult result) {
                    Assert.fail("response() called");
                }

                @Override
                public void partialFailed(String address, List<MultiFieldPoint> points, HttpServerBadRequestException ex, SummaryResult result) {
                    Assert.fail("partialFailed() whith summary called");
                }
            };

            callFailedWithResponse(createMultiFieldBatchPutHttpResponseCallback(cb), bdex);
        }

        // bad request with MultiFieldBatchPutSummaryCallback
        {
            String content = "{\"success\":6,\"failed\":3}";
            final JSONObject jsonContent = JSON.parseObject(content);
            final JSONArray jsonErrors = jsonContent.getJSONArray("errors");


            ResultResponse response = new ResultResponse(400, content);
            final HttpServerNotSupportException bdex = new HttpServerNotSupportException(response);

            AbstractMultiFieldBatchPutCallback cb = new MultiFieldBatchPutSummaryCallback() {
                @Override
                public void response(String address, List<MultiFieldPoint> points, SummaryResult result) {
                    Assert.fail("response() called");
                }

                @Override
                public void failed(String address, List<MultiFieldPoint> input, Exception ex) {
                    Assert.assertTrue(ex == bdex);
                }

                @Override
                public void partialFailed(String address, List<MultiFieldPoint> points, HttpServerBadRequestException ex, SummaryResult result) {
                    Assert.fail("failed() whith summary called");
                }
            };

            callFailedWithResponse(createMultiFieldBatchPutHttpResponseCallback(cb), bdex);
        }
    }

    private MultiFieldBatchPutHttpResponseCallback createMultiFieldBatchPutHttpResponseCallback(AbstractMultiFieldBatchPutCallback<?> multiFieldBatchPutCallback) {
        TSDBConfig config = TSDBConfig.address("127.0.0.1", 3002).config();
        HttpClient client = mock(HttpClient.class);

        MultiFieldBatchPutHttpResponseCallback callback = new MultiFieldBatchPutHttpResponseCallback("127.0.0.1", client,
                multiFieldBatchPutCallback, Collections.EMPTY_LIST,  config, 3);
        return callback;
    }

    private void callFailedWithResponse(MultiFieldBatchPutHttpResponseCallback callback, Exception ex) throws Exception {
        Whitebox.invokeMethod(callback, "failedWithResponse", ex);
    }
}
