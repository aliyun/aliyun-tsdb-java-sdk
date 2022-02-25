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
    String MAIN_CLUSTER_TIME = "/api/set_cluster?mainClusterTime";

    String UPDATE_LAST = "/api/updatelast";

    String TRUNCATE = "/api/truncate";

    String DELETE_ALL_TABLE = "/api/delete_all_table";

    String HEALTH = "/api/health";

    String VIP_HEALTH = "/api/vip_health";

    /**
     * Multi-field data model APIs
     */
    String MPUT = "/api/mput";
    String MQUERY = "/api/mquery";
    String QUERY_MLAST = "/api/query/mlast";
    /**
     * User authentication API
     */
    String USER_AUTH = "/api/users";

    String SQL = "/api/sqlquery";

    String TAGS_ADD = "/api/tags/add";
    String TAGS_SHOW = "/api/tags/show";
    String TAGS_REMOVE = "/api/tags/remove";

}
