package com.aliyun.hitsdb.client.callback;

import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.response.batch.SummaryResult;

import java.util.List;

public abstract class MultiFieldBatchPutSummaryCallback extends AbstractMultiFieldBatchPutCallback<SummaryResult> {

    @Override
    public abstract void response(String address, List<MultiFieldPoint> points, SummaryResult result);
}
