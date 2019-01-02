package com.aliyun.hitsdb.client.value.type;

public enum Suggest {
    Metrics("metrics"), Field("field"), Tagk("tagk"), Tagv("tagv");

    private String name;

    private Suggest(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

}
