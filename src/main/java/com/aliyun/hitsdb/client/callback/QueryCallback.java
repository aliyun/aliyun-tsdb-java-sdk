package com.aliyun.hitsdb.client.callback;

import java.util.List;

import com.aliyun.hitsdb.client.value.request.Query;
import com.aliyun.hitsdb.client.value.response.QueryResult;

public abstract class QueryCallback extends AbstractCallback<Query, List<QueryResult>> {

}