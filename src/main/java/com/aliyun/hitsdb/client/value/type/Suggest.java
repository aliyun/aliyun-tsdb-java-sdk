package com.aliyun.hitsdb.client.value.type;

public enum Suggest {
    Metrics("metrics"), Tagk("tagk");

    private String name;

    private Suggest(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

}
