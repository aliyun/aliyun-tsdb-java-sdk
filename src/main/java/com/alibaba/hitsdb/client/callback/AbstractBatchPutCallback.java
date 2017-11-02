package com.alibaba.hitsdb.client.callback;

import java.util.List;

import com.alibaba.hitsdb.client.value.Result;
import com.alibaba.hitsdb.client.value.request.Point;

public abstract class AbstractBatchPutCallback<R extends Result> extends AbstractCallback<List<Point>,R > {
}