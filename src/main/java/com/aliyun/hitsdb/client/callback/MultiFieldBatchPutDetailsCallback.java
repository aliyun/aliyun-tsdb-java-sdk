package com.aliyun.hitsdb.client.callback;

import com.aliyun.hitsdb.client.exception.http.HttpServerBadRequestException;
import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;
import com.aliyun.hitsdb.client.value.response.batch.MultiFieldDetailsResult;

import java.util.List;

public abstract class MultiFieldBatchPutDetailsCallback extends AbstractMultiFieldBatchPutCallback<MultiFieldDetailsResult> {

    @Override
    public abstract void response(String address, List<MultiFieldPoint> points, MultiFieldDetailsResult result);

    /**
     * the unique callback method for the scenario that error returned with detailed information of the error data points
     * if the application want to handle this scenario, it should override this method
     * @param result the detailed information contains the statistical information and error points, if any.
     *               NOTICE!! result might be NULL
     * @since 0.3.5
     */
    public void failed(String address, List<MultiFieldPoint> points, HttpServerBadRequestException ex, MultiFieldDetailsResult result) {
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
