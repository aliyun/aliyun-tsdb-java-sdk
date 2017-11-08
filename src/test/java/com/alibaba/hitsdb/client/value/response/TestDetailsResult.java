package com.alibaba.hitsdb.client.value.response;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.response.batch.DetailsResult;
import com.aliyun.hitsdb.client.value.response.batch.ErrorPoint;

public class TestDetailsResult {

    @Test
    public void testDeserialize() {
        String jsonStr = "{\"errors\": [{\"datapoint\": {\"metric\": \"sys.cpu.nice\",\"timestamp\": 1365465600,\"value\": null,\"tags\": {\"host\": \"web01\"}},\"error\": \"Unable to parse value to a number\"}],\"failed\": 1,\"success\": 0}";
        DetailsResult detailsResult = JSON.parseObject(jsonStr, DetailsResult.class);
        int failed = detailsResult.getFailed();
        int success = detailsResult.getSuccess();
        List<ErrorPoint> errors = detailsResult.getErrors();
        Assert.assertEquals(success, 0);
        Assert.assertEquals(failed, 1);
        Assert.assertEquals(errors.size(), 1);
        ErrorPoint errorPoint = errors.get(0);
        String error = errorPoint.getError();
        Point datapoint = errorPoint.getDatapoint();
        System.out.println(datapoint);
        Assert.assertEquals(error, "Unable to parse value to a number");
        Assert.assertEquals(datapoint.getMetric(), "sys.cpu.nice");
        Assert.assertEquals(datapoint.getTimestamp().intValue(), 1365465600);
        Assert.assertEquals(datapoint.toJSON(),
                "{\"metric\":\"sys.cpu.nice\",\"tags\":{\"host\":\"web01\"},\"timestamp\":1365465600}");
    }

    @Test
    public void testDeserialize2() {
        String jsonStr = "{\"errors\": [{\"datapoint\": {\"metric\": \"sys.cpu.nice\",\"timestamp\": 1365465600,\"value\": null,\"tags\": {\"host\": \"web01\"}},\"error\": \"Unable to parse value to a number\"},{\"datapoint\": {\"metric\": \"sys.cpu.nice\",\"timestamp\": 1365465600,\"value\": null,\"tags\": {\"host\": \"web01\"}},\"error\": \"Unable to parse value to a number\"}],\"failed\": 2,\"success\": 0}";
        DetailsResult detailsResult = JSON.parseObject(jsonStr, DetailsResult.class);
        int failed = detailsResult.getFailed();
        int success = detailsResult.getSuccess();
        List<ErrorPoint> errors = detailsResult.getErrors();
        Assert.assertEquals(success, 0);
        Assert.assertEquals(failed, 2);
        Assert.assertEquals(errors.size(), 2);
        {
            // 0
            ErrorPoint errorPoint = errors.get(0);
            String error = errorPoint.getError();
            Point datapoint = errorPoint.getDatapoint();
            Assert.assertEquals(error, "Unable to parse value to a number");
            Assert.assertEquals(datapoint.getMetric(), "sys.cpu.nice");
            Assert.assertEquals(datapoint.getTimestamp().intValue(), 1365465600);
            Assert.assertEquals(datapoint.toJSON(),
                    "{\"metric\":\"sys.cpu.nice\",\"tags\":{\"host\":\"web01\"},\"timestamp\":1365465600}");
        }
        {
            // 1
            ErrorPoint errorPoint = errors.get(0);
            String error = errorPoint.getError();
            Point datapoint = errorPoint.getDatapoint();
            Assert.assertEquals(error, "Unable to parse value to a number");
            Assert.assertEquals(datapoint.getMetric(), "sys.cpu.nice");
            Assert.assertEquals(datapoint.getTimestamp().intValue(), 1365465600);
            Assert.assertEquals(datapoint.toJSON(),
                    "{\"metric\":\"sys.cpu.nice\",\"tags\":{\"host\":\"web01\"},\"timestamp\":1365465600}");
        }
    }
}
