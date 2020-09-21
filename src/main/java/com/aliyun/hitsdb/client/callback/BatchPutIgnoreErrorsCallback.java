package com.aliyun.hitsdb.client.callback;

import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.response.batch.IgnoreErrorsResult;

import java.util.List;

/**
 * Copyright @ 2020 alibaba.com
 * All right reserved.
 * Function：Batch Put IgnoreErrors Callback
 *
 * @author Benedict Jin
 * @since 2020/09/21
 */
public abstract class BatchPutIgnoreErrorsCallback extends AbstractBatchPutCallback<IgnoreErrorsResult> {

    @Override
    public abstract void response(String address, List<Point> points, IgnoreErrorsResult result);
}
