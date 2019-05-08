package com.aliyun.hitsdb.client.callback;

import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;

import java.util.List;

public abstract class AbstractMultiFieldBatchPutCallback<R extends Result> extends AbstractCallback<List<MultiFieldPoint>,R > {
}