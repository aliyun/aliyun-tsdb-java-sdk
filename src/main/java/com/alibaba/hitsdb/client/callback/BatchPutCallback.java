package com.alibaba.hitsdb.client.callback;

import java.util.List;

import com.alibaba.hitsdb.client.value.Result;
import com.alibaba.hitsdb.client.value.request.Point;

public abstract class BatchPutCallback extends AbstractBatchPutCallback<Result>{

    @Override
    public abstract void response(String address, List<Point> points, Result result);

}
