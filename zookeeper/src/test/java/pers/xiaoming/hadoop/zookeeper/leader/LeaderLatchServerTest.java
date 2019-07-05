package pers.xiaoming.hadoop.zookeeper.leader;

import org.junit.BeforeClass;
import org.junit.Test;

public class LeaderLatchServerTest extends LeaderTestBase {
    @BeforeClass
    public static void setupThis() throws Exception {
        servers = new LeaderLatchServer[NUM_OF_SERVERS];

        for (int i = 0; i < NUM_OF_SERVERS; i++) {
            LeaderLatchServer server = new LeaderLatchServer("S # " + i, PATH, clients[i]);
            servers[i] = server;
        }
    }

    @Test
    public void testElection() throws Exception {
        startClients();

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
}
