package pers.xiaoming.hadoop.zookeeper.leader;

import org.apache.curator.framework.CuratorFramework;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class LeaderLatchServerTest extends LeaderTestBase {
    private static LeaderLatchServer[] servers;

    @BeforeClass
    public static void setupThis() throws Exception {
        servers = new LeaderLatchServer[NUM_OF_SERVERS];

        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            LeaderLatchServer server = new LeaderLatchServer("S # " + i, LATCH_PATH, clients[i]);
            servers[i] = server;
        }
    }

    @AfterClass
    public static void closeThis() throws IOException {
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            if (servers[i] != null) {
                servers[i].stop();
            }
        }
    }

    @Test
    public void testElection() throws Exception {
        for (CuratorFramework client : clients) {
            client.start();
        }
        assertHasLeader();

        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            if (servers[i].hasLeadership()) {
                servers[i].stop();
                servers[i] = null;
                clients[i] = null;
            }
        }

        assertHasLeader();
    }

    private void assertHasLeader() throws InterruptedException {
        // The election takes sometime,
        // Just after client start, there will be no leader elected
        Assert.assertFalse(hasLeader());

        // Leader elected after some time
        Thread.sleep(500);
        Assert.assertTrue(hasLeader());
    }

    private boolean hasLeader() {
        for (LeaderLatchServer server : servers) {
            if (server == null) continue;
            logger.info(String.format("Server %s currently is leader? %s", server.getName(), server.hasLeadership()));
            if (server.hasLeadership()) {
                return true;
            }
        }
        return false;
    }
}
