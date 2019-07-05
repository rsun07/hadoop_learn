package pers.xiaoming.hadoop.zookeeper.leader;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class LeaderTestBase {
    static final Logger logger = Logger.getLogger(LeaderLatchServerTest.class);
    static final String LATCH_PATH = "/election/leader";
    static final int NUM_OF_SERVERS = 10;
    static CuratorFramework[] clients;

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
    public static void close() {
        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            if (clients[i] != null) {
                clients[i].close();
            }
        }
    }
}
