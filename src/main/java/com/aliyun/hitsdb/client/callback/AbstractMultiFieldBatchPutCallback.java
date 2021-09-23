package com.aliyun.hitsdb.client.callback;

import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractMultiFieldBatchPutCallback<R extends Result> extends AbstractCallback<List<MultiFieldPoint>,R > {

    public static Map<String, String> getMultiFieldPutQueryParamMap(AbstractMultiFieldBatchPutCallback<?> multiFieldBatchPutCallback) {
        Map<String, String> paramsMap = new HashMap<String, String>();
        if (multiFieldBatchPutCallback != null) {
            if (multiFieldBatchPutCallback instanceof MultiFieldBatchPutCallback) {
            } else if (multiFieldBatchPutCallback instanceof MultiFieldBatchPutSummaryCallback) {
                paramsMap.put("summary", "true");
            } else if (multiFieldBatchPutCallback instanceof MultiFieldBatchPutDetailsCallback) {
                paramsMap.put("details", "true");
            } else if (multiFieldBatchPutCallback instanceof MultiFieldBatchPutIgnoreErrorsCallback) {
                paramsMap.put("ignoreErrors", "true");
            }
        }
        return paramsMap;
    }
}