package com.aliyun.hitsdb.client.http;

public interface HttpAPI {
    public final static String PUT = "/api/put";
    public final static String QUERY = "/api/query";
    public final static String QUERY_LASTDP = "/api/query/lastdp";
    public final static String TTL = "/api/ttl";
    public final static String DELETE_DATA = "/api/delete_data";
    public final static String DELETE_META = "/api/delete_meta";
    public final static String SUGGEST = "/api/suggest";
    public final static String DUMP_META = "/api/dump_meta";
}
