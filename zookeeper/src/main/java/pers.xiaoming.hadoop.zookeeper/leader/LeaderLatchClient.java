package pers.xiaoming.hadoop.zookeeper.leader;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.log4j.Logger;

import java.io.IOException;

public class LeaderLatchClient implements LeaderLatchListener {
    private static final Logger logger = Logger.getLogger(LeaderLatchClient.class);

    private final String name;

    private LeaderLatch leaderLatch;

    private CuratorFramework client;

    public LeaderLatchClient(String name, String latchPath, CuratorFramework client) throws Exception {
        this.name = name;
        this.client = client;
        this.leaderLatch = new LeaderLatch(client, latchPath);
        leaderLatch.addListener(this);
        leaderLatch.start();
    }

    @Override
    public void isLeader() {
        logger.info(name + " is elected to be leader");
    }

    @Override
    public void notLeader() {
        logger.info(name + " is no longer the leader");
    }

    public String getName() {
        return name;
    }

    public CuratorFramework getClient() {
        return client;
    }

    public void stop() throws IOException {
        leaderLatch.close();
    }

    public boolean hasLeadership() {
        return leaderLatch.hasLeadership();
    }
}
