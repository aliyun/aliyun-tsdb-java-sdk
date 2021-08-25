package com.aliyun.hitsdb.client.value.type;

import java.util.HashMap;
import java.util.Map;

public enum DownsampleDataSource {
    RAW("raw"),
    DOWNSAMPLE("downsample");

    private final String name;

    private DownsampleDataSource(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
