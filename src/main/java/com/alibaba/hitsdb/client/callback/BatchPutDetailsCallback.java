package com.alibaba.hitsdb.client.callback;

import java.util.List;

import com.alibaba.hitsdb.client.value.request.Point;
import com.alibaba.hitsdb.client.value.response.batch.DetailsResult;

public abstract class BatchPutDetailsCallback extends AbstractBatchPutCallback<DetailsResult> {

    @Override
    public abstract void response(String address,List<Point> points, DetailsResult result);

}
