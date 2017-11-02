package com.alibaba.hitsdb.client.callback;

import java.util.List;

import com.alibaba.hitsdb.client.value.request.Query;
import com.alibaba.hitsdb.client.value.response.QueryResult;

public abstract class QueryCallback extends AbstractCallback<Query,List<QueryResult>> {
    
}