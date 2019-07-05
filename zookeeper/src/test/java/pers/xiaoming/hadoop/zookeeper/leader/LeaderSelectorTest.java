package pers.xiaoming.hadoop.zookeeper.leader;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class LeaderSelectorTest extends LeaderTestBase {
    @BeforeClass
    public static void setupThis() {
        servers = new LeaderSelectorServer[NUM_OF_SERVERS];

        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            LeaderSelectorServer server = new LeaderSelectorServer("S # " + i, PATH, clients[i]);
            servers[i] = server;
        }
    }

    @Test
    public void testElection() throws InterruptedException, IOException {
        startClients();

        int leaderId = assertHasLeader();

        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            if (servers[i].hasLeadership()) {
                LeaderSelectorServer server = (LeaderSelectorServer) servers[i];
                server.stopLead();
            }
        }
        int leaderId2 = assertHasLeader(true);
        Assert.assertSame(leaderId, leaderId2);

        Thread.sleep(1000);

        // lead should change after the sleep
        int leaderId3 = assertHasLeader(true);
        Assert.assertNotSame(leaderId, leaderId3);
    }
}
