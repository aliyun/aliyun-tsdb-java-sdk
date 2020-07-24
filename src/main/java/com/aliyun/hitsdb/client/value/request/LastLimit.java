package com.aliyun.hitsdb.client.value.request;

/**
 * @author cuiyuan
 * @date 2020/7/22 12:05 下午
 */
public class LastLimit {
    long from;
    int size;

    public LastLimit(long from, int size) {
        this.from = from;
        this.size = size;
    }

    public long getFrom() {
        return from;
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
