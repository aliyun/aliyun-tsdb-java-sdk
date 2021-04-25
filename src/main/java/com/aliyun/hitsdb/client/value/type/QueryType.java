package com.aliyun.hitsdb.client.value.type;

import java.util.HashMap;
import java.util.Map;

public enum QueryType {
    ALL("ALL"),
    DOUBLE("DOUBLE"),
    STRING("STRING"),
    LATEST("LATEST"),
    UNKNOWN("UNKNOWN");

    private static final Map<String, QueryType> CODE_MAP = new HashMap<String, QueryType>();

    static {
        for (QueryType typeEnum : QueryType.values()) {
            CODE_MAP.put(typeEnum.getName(), typeEnum);
        }
    }

    public static QueryType getEnum(String name) {
        return CODE_MAP.get(name);
    }

    private String name;

    private QueryType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }
}
