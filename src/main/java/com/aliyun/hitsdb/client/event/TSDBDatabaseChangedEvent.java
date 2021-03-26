package com.aliyun.hitsdb.client.event;

import java.util.EventObject;

public final class TSDBDatabaseChangedEvent extends EventObject {
    private static final long serialVersionUID = 8322760240914087616L;

    private String previousDatabase;
    private String currentDatabase;

    public TSDBDatabaseChangedEvent(Object source, String previousDatabase, String currentDatabase) {
        super(source);
        this.previousDatabase = previousDatabase;
        this.currentDatabase = currentDatabase;
    }

    public String getPreviousDatabase() {
        return previousDatabase;
    }

    public String getCurrentDatabase() {
        return currentDatabase;
    }
}
