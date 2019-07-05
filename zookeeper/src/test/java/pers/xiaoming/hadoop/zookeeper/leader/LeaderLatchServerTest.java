package pers.xiaoming.hadoop.zookeeper.leader;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class LeaderLatchServerTest {
    private static final Logger logger = Logger.getLogger(LeaderLatchServerTest.class);
    private static final String LATCH_PATH = "/election/leader";
    private static final int NUM_OF_SERVERS = 10;
    private static TestingServer testingServer;
    private static RetryPolicy retryPolicy;
    private static CuratorFramework[] clients;
    private static LeaderLatchServer[] servers;

    @BeforeClass
    public static void setup() throws Exception {
        testingServer = new TestingServer();
        retryPolicy = new ExponentialBackoffRetry(1000, 3);
        clients = new CuratorFramework[NUM_OF_SERVERS];
        servers = new LeaderLatchServer[NUM_OF_SERVERS];

        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            CuratorFramework client = CuratorFrameworkFactory.newClient(testingServer.getConnectString(), retryPolicy);
            clients[i] = client;

            LeaderLatchServer server = new LeaderLatchServer("S # " + i, LATCH_PATH, client);
            servers[i] = server;
        }
    }

    @AfterClass
    public static void close() throws IOException {
//        for (int i = 0; i < NUM_OF_SERVERS; i++) {
//            if (servers[i] != null) {
//                servers[i].stop();
//            }
//            if (clients[i] != null) {
//                clients[i].close();
//            }
//        }
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
