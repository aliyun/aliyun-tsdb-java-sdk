package com.aliyun.hitsdb.client.callback;

import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;
import com.aliyun.hitsdb.client.value.response.batch.MultiFieldIgnoreErrorsResult;

import java.util.List;

/**
 * Copyright @ 2020 alibaba.com
 * All right reserved.
 * Function：MultiField Batch Put IgnoreErrors Callback
 *
 * @author Benedict Jin
 * @since 2020/09/21
 */
public abstract class MultiFieldBatchPutIgnoreErrorsCallback extends AbstractMultiFieldBatchPutCallback<MultiFieldIgnoreErrorsResult> {

    @Override
    public abstract void response(String address, List<MultiFieldPoint> points, MultiFieldIgnoreErrorsResult result);
}
