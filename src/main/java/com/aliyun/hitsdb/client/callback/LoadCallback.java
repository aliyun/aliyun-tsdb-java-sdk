package com.aliyun.hitsdb.client.callback;

import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.CompressionBatchPoints;

public abstract class LoadCallback extends AbstractCallback<CompressionBatchPoints, Result> {
    
    @Override
    public abstract void response(String address, CompressionBatchPoints points, Result result);
}