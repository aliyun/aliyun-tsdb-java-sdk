package com.aliyun.hitsdb.client.callback;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.Point;

public abstract class AbstractBatchPutCallback<R extends Result> extends AbstractCallback<List<Point>,R > {

    public static Map<String, String> getPutQueryParamMap(AbstractBatchPutCallback<?> batchPutCallback) {
        Map<String, String> paramsMap = new HashMap<String, String>();
        if (batchPutCallback != null) {
            if (batchPutCallback instanceof BatchPutCallback) {
            } else if (batchPutCallback instanceof BatchPutSummaryCallback) {
                paramsMap.put("summary", "true");
            } else if (batchPutCallback instanceof BatchPutDetailsCallback) {
                paramsMap.put("details", "true");
            } else if (batchPutCallback instanceof BatchPutIgnoreErrorsCallback) {
                paramsMap.put("ignoreErrors", "true");
            }
        }
        return paramsMap;
    }
}