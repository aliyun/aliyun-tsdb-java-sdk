package com.aliyun.hitsdb.client.callback;

import java.util.List;

import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.Point;

public abstract class AbstractBatchPutCallback<R extends Result> extends AbstractCallback<List<Point>,R > {
}