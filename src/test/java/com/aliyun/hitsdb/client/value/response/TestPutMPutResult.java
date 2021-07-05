package com.aliyun.hitsdb.client.value.response;

import com.aliyun.hitsdb.client.TSDB;
import com.aliyun.hitsdb.client.TSDBClient;
import com.aliyun.hitsdb.client.TSDBConfig;
import com.aliyun.hitsdb.client.exception.http.HttpClientException;
import com.aliyun.hitsdb.client.http.HttpAPI;
import com.aliyun.hitsdb.client.http.HttpClient;
import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.response.batch.*;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.protocol.HTTP;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDBClient.class, TSDBConfig.class, HttpClient.class, HttpResponse.class})
public class TestPutMPutResult {

    @Test
    public void testMultiFieldPutSync() throws Exception {

        {
            TSDB tsdbClient = mockTSDBClient(HttpAPI.MPUT, 200, "{\"failed\":0,\"success\":1}");

            final MultiFieldPoint mp = MultiFieldPoint.metric("test")
                    .tag("tag1", "tag1v1")
                    .timestamp(System.currentTimeMillis())
                    .field("f1", 1.0)
                    .field("f2", 2.0)
                    .build();
            List<MultiFieldPoint> mps = new ArrayList<MultiFieldPoint>() {{
                add(mp);
            }};

            Result summaryResult = tsdbClient.multiFieldPutSync(mps, Result.class);

            Assert.assertNull(summaryResult);
        }

        {
            TSDB tsdbClient = mockTSDBClient(HttpAPI.MPUT, 200, "{\"failed\":0,\"success\":1}");

            final MultiFieldPoint mp = MultiFieldPoint.metric("test")
                    .tag("tag1", "tag1v1")
                    .timestamp(System.currentTimeMillis())
                    .field("f1", 1.0)
                    .field("f2", 2.0)
                    .build();
            List<MultiFieldPoint> mps = new ArrayList<MultiFieldPoint>() {{
                add(mp);
            }};

            SummaryResult summaryResult = tsdbClient.multiFieldPutSync(mps, SummaryResult.class);

            Assert.assertEquals(1, summaryResult.getSuccess());
            Assert.assertEquals(0, summaryResult.getFailed());
        }

        {
            TSDB tsdbClient = mockTSDBClient(HttpAPI.MPUT, 200, "{\"failed\":1,\"success\":1, \"errors\":[{\"error\": \"mock message\", \"datapoint\":{\"metric\":\"test\", \"tags\":{\"tag1\":\"tag1v1\"}, \"timestamp\":1625471027, \"fields\":{\"f1\":1.0,\"f2\":2.0}}}]}");

            final MultiFieldPoint mp = MultiFieldPoint.metric("test")
                    .tag("tag1", "tag1v1")
                    .timestamp(System.currentTimeMillis())
                    .field("f1", 1.0)
                    .field("f2", 2.0)
                    .build();
            List<MultiFieldPoint> mps = new ArrayList<MultiFieldPoint>() {{
                add(mp);
            }};

            MultiFieldDetailsResult summaryResult = tsdbClient.multiFieldPutSync(mps, MultiFieldDetailsResult.class);

            Assert.assertEquals(1, summaryResult.getSuccess());
            Assert.assertEquals(1, summaryResult.getFailed());
            Assert.assertNotNull(summaryResult.getErrors());
            Assert.assertFalse(summaryResult.getErrors().isEmpty());
        }

        {
            TSDB tsdbClient = mockTSDBClient(HttpAPI.MPUT, 200, "{\"failed\":1,\"success\":1, \"ignoredErrors\":[{\"error\": \"mock message\", \"datapoint\":{\"metric\":\"test\", \"tags\":{\"tag1\":\"tag1v1\"}, \"timestamp\":1625471027, \"fields\":{\"f1\":1.0,\"f2\":2.0}}}]}");

            final MultiFieldPoint mp = MultiFieldPoint.metric("test")
                    .tag("tag1", "tag1v1")
                    .timestamp(System.currentTimeMillis())
                    .field("f1", 1.0)
                    .field("f2", 2.0)
                    .build();
            List<MultiFieldPoint> mps = new ArrayList<MultiFieldPoint>() {{
                add(mp);
            }};

            MultiFieldIgnoreErrorsResult summaryResult = tsdbClient.multiFieldPutSync(mps, MultiFieldIgnoreErrorsResult.class);

            Assert.assertEquals(1, summaryResult.getSuccess());
            Assert.assertEquals(1, summaryResult.getFailed());
            Assert.assertNotNull(summaryResult.getIgnoredErrors());
            Assert.assertFalse(summaryResult.getIgnoredErrors().isEmpty());
        }

        {
            TSDB tsdbClient = mockTSDBClient(HttpAPI.MPUT, 200, "{\"failed\":1,\"success\":1, \"errors\":[{\"error\": \"mock message\", \"datapoint\":{\"metric\":\"test\", \"tags\":{\"tag1\":\"tag1v1\"}, \"timestamp\":1625471027, \"fields\":{\"f1\":1.0,\"f2\":2.0}}}]}");

            final MultiFieldPoint mp = MultiFieldPoint.metric("test")
                    .tag("tag1", "tag1v1")
                    .timestamp(System.currentTimeMillis())
                    .field("f1", 1.0)
                    .field("f2", 2.0)
                    .build();
            List<MultiFieldPoint> mps = new ArrayList<MultiFieldPoint>() {{
                add(mp);
            }};

            try {
                tsdbClient.multiFieldPutSync(mps, IgnoreErrorsResult.class);
                Assert.fail("IgnoreErrorsResult should not be supported");
            } catch (HttpClientException ex) {
                Assert.assertTrue(ex.getMessage().contains("not supported"));
            }
        }

        {
            TSDB tsdbClient = mockTSDBClient(HttpAPI.MPUT, 200, "{\"failed\":1,\"success\":1, \"errors\":[{\"error\": \"mock message\", \"datapoint\":{\"metric\":\"test\", \"tags\":{\"tag1\":\"tag1v1\"}, \"timestamp\":1625471027, \"fields\":{\"f1\":1.0,\"f2\":2.0}}}]}");

            final MultiFieldPoint mp = MultiFieldPoint.metric("test")
                    .tag("tag1", "tag1v1")
                    .timestamp(System.currentTimeMillis())
                    .field("f1", 1.0)
                    .field("f2", 2.0)
                    .build();
            List<MultiFieldPoint> mps = new ArrayList<MultiFieldPoint>() {{
                add(mp);
            }};

            try {
                tsdbClient.multiFieldPutSync(mps, DetailsResult.class);
                Assert.fail("DetailsResult should not be supported");
            } catch (HttpClientException ex) {
                Assert.assertTrue(ex.getMessage().contains("not supported"));
            }
        }
    }

    @Test
    public void testPutSync() throws Exception {

        {
            TSDB tsdbClient = mockTSDBClient(HttpAPI.PUT, 200, "{\"failed\":0,\"success\":1}");

            final Point mp = Point.metric("test")
                    .tag("tag1", "tag1v1")
                    .timestamp(System.currentTimeMillis())
                    .value(1.0)
                    .build();
            List<Point> mps = new ArrayList<Point>() {{
                add(mp);
            }};

            Result summaryResult = tsdbClient.putSync(mps, Result.class);

            Assert.assertNull(summaryResult);
        }

        {
            TSDB tsdbClient = mockTSDBClient(HttpAPI.PUT, 200, "{\"failed\":0,\"success\":1}");

            final Point mp = Point.metric("test")
                    .tag("tag1", "tag1v1")
                    .timestamp(System.currentTimeMillis())
                    .value(1.0)
                    .build();
            List<Point> mps = new ArrayList<Point>() {{
                add(mp);
            }};

            SummaryResult summaryResult = tsdbClient.putSync(mps, SummaryResult.class);

            Assert.assertEquals(1, summaryResult.getSuccess());
            Assert.assertEquals(0, summaryResult.getFailed());
        }

        {
            TSDB tsdbClient = mockTSDBClient(HttpAPI.PUT, 200, "{\"failed\":1,\"success\":1, \"errors\":[{\"error\": \"mock message\", \"datapoint\":{\"metric\":\"test\", \"tags\":{\"tag1\":\"tag1v1\"}, \"timestamp\":1625471027, \"value\":1.0}}]}");

            final Point mp = Point.metric("test")
                    .tag("tag1", "tag1v1")
                    .timestamp(System.currentTimeMillis())
                    .value(1.0)
                    .build();
            List<Point> mps = new ArrayList<Point>() {{
                add(mp);
            }};

            DetailsResult summaryResult = tsdbClient.putSync(mps, DetailsResult.class);

            Assert.assertEquals(1, summaryResult.getSuccess());
            Assert.assertEquals(1, summaryResult.getFailed());
            Assert.assertNotNull(summaryResult.getErrors());
            Assert.assertFalse(summaryResult.getErrors().isEmpty());
        }

        {
            TSDB tsdbClient = mockTSDBClient(HttpAPI.PUT, 200, "{\"failed\":1,\"success\":1, \"ignoredErrors\":[{\"error\": \"mock message\", \"datapoint\":{\"metric\":\"test\", \"tags\":{\"tag1\":\"tag1v1\"}, \"timestamp\":1625471027, \"value\":1.0}}]}");

            final Point mp = Point.metric("test")
                    .tag("tag1", "tag1v1")
                    .timestamp(System.currentTimeMillis())
                    .value(1.0)
                    .build();
            List<Point> mps = new ArrayList<Point>() {{
                add(mp);
            }};

            IgnoreErrorsResult summaryResult = tsdbClient.putSync(mps, IgnoreErrorsResult.class);

            Assert.assertEquals(1, summaryResult.getSuccess());
            Assert.assertEquals(1, summaryResult.getFailed());
            Assert.assertNotNull(summaryResult.getIgnoredErrors());
            Assert.assertFalse(summaryResult.getIgnoredErrors().isEmpty());
        }

        {
            TSDB tsdbClient = mockTSDBClient(HttpAPI.PUT, 200, "{\"failed\":1,\"success\":1, \"ignoredErrors\":[{\"error\": \"mock message\", \"datapoint\":{\"metric\":\"test\", \"tags\":{\"tag1\":\"tag1v1\"}, \"timestamp\":1625471027, \"value\":1.0}}]}");

            final Point mp = Point.metric("test")
                    .tag("tag1", "tag1v1")
                    .timestamp(System.currentTimeMillis())
                    .value(1.0)
                    .build();
            List<Point> mps = new ArrayList<Point>() {{
                add(mp);
            }};

            try {
                tsdbClient.putSync(mps, MultiFieldDetailsResult.class);
                Assert.fail("MultiFieldDetailsResult should not be supported");
            } catch (HttpClientException ex) {
                Assert.assertTrue(ex.getMessage().contains("not supported"));
            }
        }

        {
            TSDB tsdbClient = mockTSDBClient(HttpAPI.PUT, 200, "{\"failed\":1,\"success\":1, \"ignoredErrors\":[{\"error\": \"mock message\", \"datapoint\":{\"metric\":\"test\", \"tags\":{\"tag1\":\"tag1v1\"}, \"timestamp\":1625471027, \"value\":1.0}}]}");

            final Point mp = Point.metric("test")
                    .tag("tag1", "tag1v1")
                    .timestamp(System.currentTimeMillis())
                    .value(1.0)
                    .build();
            List<Point> mps = new ArrayList<Point>() {{
                add(mp);
            }};

            try {
                tsdbClient.putSync(mps, MultiFieldIgnoreErrorsResult.class);
                Assert.fail("MultiFieldIgnoreErrorsResult should not be supported");
            } catch (HttpClientException ex) {
                Assert.assertTrue(ex.getMessage().contains("not supported"));
            }
        }
    }


    private TSDB mockTSDBClient(final String putURL, final int statusCode, final String respContent) throws Exception {
        TSDBClient client = PowerMockito.mock(TSDBClient.class);
        HttpClient httpClient = PowerMockito.mock(HttpClient.class);
        TSDBConfig config = PowerMockito.mock(TSDBConfig.class);
        PowerMockito.when(config.isDeduplicationEnable()).thenReturn(false);

        // mock the response with the given status code and content
        BasicStatusLine statusLine = new BasicStatusLine(HttpVersion.HTTP_1_1, statusCode, "");

        BasicHttpEntity entity = new BasicHttpEntity();
        entity.setContent(new ByteArrayInputStream(respContent.getBytes()));
        entity.setContentType(new BasicHeader(HTTP.CONTENT_TYPE, "text/plain"));

        HttpResponse response = PowerMockito.mock(HttpResponse.class);
        PowerMockito.when(response.getStatusLine()).thenReturn(statusLine);
        PowerMockito.when(response.getEntity()).thenReturn(entity);
        PowerMockito.when(response.getHeaders(Matchers.endsWith("Content-Encoding"))).thenReturn(new Header[] { new BasicHeader(HTTP.CONTENT_ENCODING, "identity")});

        if (putURL.equals(HttpAPI.MPUT) || putURL.equals(HttpAPI.PUT)) {
            PowerMockito.when(httpClient.post(Mockito.anyString(), Mockito.anyString(), Mockito.anyMap())).thenReturn(response);
        } else {
            throw new IllegalArgumentException(putURL + "not supported");
        }

        // set the internal variable of TSDBClient
        Whitebox.setInternalState(client, "httpclient", httpClient);
        Whitebox.setInternalState(client, "httpCompress", false);
        Whitebox.setInternalState(client, "config", config);

        PowerMockito.when(client.getCurrentDatabase()).thenReturn("default");

        PowerMockito.when(client.multiFieldPutSync(Mockito.anyCollectionOf(MultiFieldPoint.class), Mockito.any(Class.class))).thenCallRealMethod();

        // mock the non-public method
        //PowerMockito.when(client, "multiFieldPutSync", Mockito.anyString(),Mockito.anyCollectionOf(MultiFieldPoint.class), Mockito.any(Class.class)).thenCallRealMethod();
        PowerMockito.when(client.multiFieldPutSync(Mockito.anyString(),Mockito.anyCollectionOf(MultiFieldPoint.class), Mockito.any(Class.class))).thenCallRealMethod();

        PowerMockito.when(client.putSync(Mockito.anyCollectionOf(Point.class), Mockito.any(Class.class))).thenCallRealMethod();

        // mock the non-public method
        //PowerMockito.when(client, "putSync", Mockito.anyString(),Mockito.anyCollectionOf(Point.class),Mockito.any(Class.class)).thenCallRealMethod();
        PowerMockito.when(client.putSync(Mockito.anyString(),Mockito.anyCollectionOf(Point.class),Mockito.any(Class.class))).thenCallRealMethod();

        return client;
    }
}
