package com.aliyun.hitsdb.client.value;

import com.aliyun.hitsdb.client.TSDBClient;
import com.aliyun.hitsdb.client.event.TSDBDatabaseChangedEvent;
import com.aliyun.hitsdb.client.event.TSDBDatabaseChangedListener;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.powermock.api.mockito.PowerMockito.doCallRealMethod;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDBClient.class})
public class TestDatabaseChange {
    private boolean testUseDatabaseCalled = false;
    private boolean testUseSameDatabaseCalled = false;

    @Test
    public void testUseDatabase() {
        {
            final String newDatabase = "newDatabase", defaultDatabase = "default";

            TSDBClient c = mockTSDBClient(defaultDatabase);

            TSDBDatabaseChangedListener listener = new TSDBDatabaseChangedListener() {
                @Override
                public void databaseChanged(TSDBDatabaseChangedEvent event) {
                    testUseDatabaseCalled = true;
                    Assert.assertTrue(newDatabase.equals(event.getCurrentDatabase()));
                    Assert.assertTrue(defaultDatabase.equals(event.getPreviousDatabase()));
                }
            };
            List<TSDBDatabaseChangedListener> subscribers = Whitebox.getInternalState(c, "listeners");

            c.addDatabaseChangedListener(listener);
            Assert.assertEquals(1, subscribers.size());

            c.useDatabase(newDatabase);

            Assert.assertTrue(newDatabase.equals(c.getCurrentDatabase()));
            Assert.assertTrue(testUseDatabaseCalled);

            c.removeDatabaseChangedListener(listener);
            Assert.assertEquals(0, subscribers.size());
        }

        // null or empty string
        {
            final String newDatabase = "", defaultDatabase = "default";
            TSDBClient c = mockTSDBClient(defaultDatabase);

            try {
                c.useDatabase(newDatabase);
                Assert.fail("\"" + newDatabase + "\" should not be allowed in useDatabase()");
            } catch (IllegalArgumentException iex) {
                System.err.println(iex.getMessage());
            } catch (Exception ex) {
                Assert.fail(String.format("unexpected exception: %s, message: %s", ex.getClass().getName(), ex.getMessage()));
            }
        }

        {
            final String newDatabase = null, defaultDatabase = "default";
            TSDBClient c = mockTSDBClient(defaultDatabase);

            try {
                c.useDatabase(newDatabase);
                Assert.fail("\"" + newDatabase + "\" should not be allowed in useDatabase()");
            } catch (IllegalArgumentException iex) {
                System.err.println(iex.getMessage());
            } catch (Exception ex) {
                Assert.fail(String.format("unexpected exception: %s, message: %s", ex.getClass().getName(), ex.getMessage()));
            }
        }
    }

    @Test
    public void testUseSameDatabase() {
        final String newDatabase = "default", defaultDatabase = "default";

        TSDBClient c = mockTSDBClient(defaultDatabase);

        TSDBDatabaseChangedListener listener = new TSDBDatabaseChangedListener() {
            @Override
            public void databaseChanged(TSDBDatabaseChangedEvent event) {
                testUseSameDatabaseCalled = true;
                Assert.assertTrue(newDatabase.equals(event.getCurrentDatabase()));
                Assert.assertTrue(defaultDatabase.equals(event.getPreviousDatabase()));
            }
        };
        List<TSDBDatabaseChangedListener> subscribers = Whitebox.getInternalState(c, "listeners");

        c.addDatabaseChangedListener(listener);
        Assert.assertEquals(1, subscribers.size());

        c.useDatabase(newDatabase);

        Assert.assertFalse(String.format("callback is not supposed to be triggered since the same database used"),testUseSameDatabaseCalled);

        c.removeDatabaseChangedListener(listener);
        Assert.assertEquals(0, subscribers.size());
    }

    private TSDBClient mockTSDBClient(final String defaultDatabase) {
        TSDBClient client = mock(TSDBClient.class);
        // set the state of internal variables
        Whitebox.setInternalState(client, "listeners", new ArrayList<TSDBDatabaseChangedListener>());
        Whitebox.setInternalState(client, "currentDatabase", new AtomicReference<String>(defaultDatabase));
        // define the behaviors
        doCallRealMethod().when(client).notifyDatabaseChanged(anyString(), anyString());
        doCallRealMethod().when(client).useDatabase(anyString());
        doCallRealMethod().when(client).addDatabaseChangedListener((TSDBDatabaseChangedListener) any());
        doCallRealMethod().when(client).removeDatabaseChangedListener((TSDBDatabaseChangedListener) any());
        when(client.getCurrentDatabase()).thenCallRealMethod();

        return client;
    }
}
