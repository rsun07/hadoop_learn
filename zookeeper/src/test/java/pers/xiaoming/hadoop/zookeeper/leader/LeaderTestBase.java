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

import java.io.IOException;

public class LeaderTestBase {
    static final Logger logger = Logger.getLogger(LeaderLatchServerTest.class);
    static final String PATH = "/election/leader";
    static final int NUM_OF_SERVERS = 10;
    static CuratorFramework[] clients;
    static LeaderServer[] servers;

    @BeforeClass
    public static void setup() throws Exception {
        TestingServer testingServer = new TestingServer();
        clients = new CuratorFramework[NUM_OF_SERVERS];

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            CuratorFramework client = CuratorFrameworkFactory.newClient(testingServer.getConnectString(), retryPolicy);
            clients[i] = client;
        }
    }

    @AfterClass
    public static void close() throws IOException {
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            if (servers[i] != null) {
                servers[i].stop();
            }
            if (clients[i] != null) {
                clients[i].close();
            }
        }
    }

    void startClients() {
        for (CuratorFramework client : clients) {
            client.start();
        }
    }

    int assertHasLeader() throws InterruptedException {
        // The election takes sometime,
        // Just after client start, there will be no leader elected
        checkLeader(false);

        // Leader elected after some time
        Thread.sleep(500);
        return checkLeader(true);
    }

    int assertHasLeader(boolean hasLead) {
        if (hasLead) {
            return checkLeader(hasLead);
        } else {
            return checkLeader(hasLead);
        }
    }

    private int checkLeader(boolean expectedHasLeader) {
        boolean hasLeader = false;
        int leaderId = -1;

        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            if (servers[i] == null) continue;
            logger.info(String.format("Server %s currently is leader? %s", servers[i].getName(), servers[i].hasLeadership()));
            if (servers[i].hasLeadership()) {
                hasLeader = true;
                leaderId = i;
                break;
            }
        }
        Assert.assertSame(expectedHasLeader, hasLeader);
        return leaderId;
    }
}
