package com.aliyun.hitsdb.client.event;

import java.util.EventListener;

public interface TSDBDatabaseChangedListener extends EventListener {
    /**
     * for the event listener implementation to run its own logic
     * @param event
     */
    void databaseChanged(TSDBDatabaseChangedEvent event);
}
