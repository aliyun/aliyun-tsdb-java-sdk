package com.aliyun.hitsdb.client.callback;

import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;

import java.util.List;

public abstract class MultiFieldBatchPutCallback extends AbstractMultiFieldBatchPutCallback<Result>{

    @Override
    public abstract void response(String address, List<MultiFieldPoint> points, Result result);

}
