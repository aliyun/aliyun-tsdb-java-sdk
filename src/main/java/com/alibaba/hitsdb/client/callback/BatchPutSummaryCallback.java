package com.alibaba.hitsdb.client.callback;

import java.util.List;

import com.alibaba.hitsdb.client.value.request.Point;
import com.alibaba.hitsdb.client.value.response.batch.SummaryResult;

public abstract class BatchPutSummaryCallback extends AbstractBatchPutCallback<SummaryResult> {

    @Override
    public abstract void response(String address, List<Point> points, SummaryResult result);
}
