package com.aliyun.hitsdb.client.http;

public interface HttpAPI {
    String PUT = "/api/put";
    String QUERY = "/api/query";
    String QUERY_LAST = "/api/query/last";
    String TTL = "/api/ttl";
    String DELETE_DATA = "/api/delete_data";
    String DELETE_META = "/api/delete_meta";
    String SUGGEST = "/api/suggest";
    String DUMP_META = "/api/dump_meta";
    String LOOKUP = "/api/search/lookup";
    String VERSION = "/api/version";

    String UPDATE_LAST = "/api/updatelast";

    String TRUNCATE = "/api/truncate";

    String DELETE_ALL_TABLE = "/api/delete_all_table";

    String VIP_HEALTH = "/api/vip_health";

    /**
     * Multi-field data model APIs
     */
    String MPUT = "/api/mput";
    String MQUERY = "/api/mquery";
    String QUERY_MLAST = "/api/query/mlast";
}
