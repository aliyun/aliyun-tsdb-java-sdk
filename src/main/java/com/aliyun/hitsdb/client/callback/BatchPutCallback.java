package com.aliyun.hitsdb.client.callback;

import java.util.List;

import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.Point;

public abstract class BatchPutCallback extends AbstractBatchPutCallback<Result>{

    @Override
    public abstract void response(String address, List<Point> points, Result result);

}
