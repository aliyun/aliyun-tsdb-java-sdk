package com.aliyun.hitsdb.client;

import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.exception.http.HttpClientException;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.http.HttpAPI;
import com.aliyun.hitsdb.client.http.response.ResultResponse;
import com.aliyun.hitsdb.client.value.request.DeleteMetaRequest;
import com.aliyun.hitsdb.client.value.request.Timeline;
import org.apache.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LindormTSDBClient extends TSDBClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(LindormTSDBClient.class);

    public LindormTSDBClient(Config config) throws HttpClientInitException {
        super(config);
    }

    public void deleteData(String metric) {
        Map request = new HashMap();
        request.put("metric", metric);
        HttpResponse httpResponse = httpclient.post(HttpAPI.DELETE_DATA, JSON.toJSONString(request));
        ResultResponse resultResponse = ResultResponse.simplify(httpResponse, this.httpCompress);
        handleVoid(resultResponse);
    }

    @Override
    public void deleteData(String metric, long startTime, long endTime) {
        throw new HttpClientException("delete data with time range is not supported");
    }

    @Override
    public void deleteData(String metric, Map<String, String> tags, long startTime, long endTime) {
        throw new HttpClientException("delete data with time range is not supported");
    }

    @Override
    public void deleteData(String metric, List<String> fields, long startTime, long endTime) {
        throw new HttpClientException("delete data with time range is not supported");
    }

    @Override
    public void deleteData(String metric, Map<String, String> tags, List<String> fields, long startTime, long endTime) {
        throw new HttpClientException("delete data with time range is not supported");
    }

    @Override
    public void deleteMeta(String metric, Map<String, String> tags) {
        throw new HttpClientException("delete meta is not supported");
    }

    @Override
    public void deleteMeta(String metric, List<String> fields, Map<String, String> tags) {
        throw new HttpClientException("delete meta is not supported");
    }

    @Override
    public void deleteMeta(Timeline timeline) {
        throw new HttpClientException("delete meta is not supported");
    }

    @Override
    public void deleteMeta(String metric, Map<String, String> tags, boolean deleteData, boolean recursive) {
        throw new HttpClientException("delete meta is not supported");
    }

    @Override
    public void deleteMeta(String metric, List<String> fields, Map<String, String> tags, boolean deleteData, boolean recursive) {
        throw new HttpClientException("delete meta is not supported");
    }

    @Override
    public void deleteMeta(DeleteMetaRequest request) {
        throw new HttpClientException("delete meta is not supported");
    }


}
