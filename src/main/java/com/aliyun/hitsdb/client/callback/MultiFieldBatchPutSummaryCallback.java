package com.aliyun.hitsdb.client.callback;

import com.aliyun.hitsdb.client.exception.http.HttpServerBadRequestException;
import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;
import com.aliyun.hitsdb.client.value.response.batch.SummaryResult;

import java.util.List;

public abstract class MultiFieldBatchPutSummaryCallback extends AbstractMultiFieldBatchPutCallback<SummaryResult> {

    @Override
    public abstract void response(String address, List<MultiFieldPoint> points, SummaryResult result);

    /**
     * the unique callback method for the scenario that error returned with summary information
     * @param result the detailed information contains the statistical information and error points, if any.
     *               NOTICE!! result might be NULL
     * @since 0.3.5
     */
    public void failed(String address, List<MultiFieldPoint> points, HttpServerBadRequestException ex, SummaryResult result) {
        // we CANNOT declare this method abstract because it would cause the application fail in the compile
        // once the application just updated the version of the dependency to the aliyun-tsdb-java-sdk
        //
        // however, we CANNOT declare a default method in the interface com.aliyun.hitsdb.client.callback.Callback either,
        // because we have to keep the compatibility to the language specification of java 6
        //
        // as a result, we make this method call the existing failed() method as its default behavior
        failed(address, points, ex);
    }
}
