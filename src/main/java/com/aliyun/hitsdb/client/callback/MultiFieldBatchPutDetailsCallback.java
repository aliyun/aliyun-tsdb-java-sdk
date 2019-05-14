package com.aliyun.hitsdb.client.callback;

import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.response.batch.DetailsResult;

import java.util.List;

public abstract class MultiFieldBatchPutDetailsCallback extends AbstractMultiFieldBatchPutCallback<DetailsResult> {

    @Override
    public abstract void response(String address, List<MultiFieldPoint> points, DetailsResult result);

}
