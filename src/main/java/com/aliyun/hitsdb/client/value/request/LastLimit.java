package com.aliyun.hitsdb.client.value.request;

/**
 * @author cuiyuan
 * @date 2020/7/22 12:05 下午
 */
public class LastLimit {
    long from;
    int size;
    boolean global;

    public LastLimit(long from, int size) {
        this.from = from;
        this.size = size;
        this.global = false;
    }

    public LastLimit(long from, int size, boolean global) {
        this.from = from;
        this.size = size;
        this.global = global;
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

    public boolean isGlobal() {
        return global;
    }

    public void setGlobal(boolean global) {
        this.global = global;
    }
}
