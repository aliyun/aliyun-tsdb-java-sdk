package com.aliyun.hitsdb.client.callback;

import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;
import com.aliyun.hitsdb.client.value.response.batch.MultiFieldDetailsResult;

import java.util.List;

public abstract class MultiFieldBatchPutDetailsCallback extends AbstractMultiFieldBatchPutCallback<MultiFieldDetailsResult> {

    @Override
    public abstract void response(String address, List<MultiFieldPoint> points, MultiFieldDetailsResult result);

}
