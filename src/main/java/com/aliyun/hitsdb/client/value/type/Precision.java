package com.aliyun.hitsdb.client.value.type;

import java.util.HashMap;
import java.util.Map;

public enum Precision {
    Second("s"),
    Millisecond("ms");

    private final String name;

    private Precision(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
