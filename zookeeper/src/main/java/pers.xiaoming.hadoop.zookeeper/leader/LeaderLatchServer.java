package pers.xiaoming.hadoop.zookeeper.leader;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.log4j.Logger;

import java.io.IOException;

public class LeaderLatchServer implements LeaderServer, LeaderLatchListener {
    private static final Logger logger = Logger.getLogger(LeaderLatchServer.class);

    private final String name;

    // Abstraction to select a "leader" amongst multiple contenders
    // in a group of JMVs connected to a Zookeeper cluster.
    private LeaderLatch leaderLatch;

    public LeaderLatchServer(String name, String latchPath, CuratorFramework client) throws Exception {
        this.name = name;
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

    public void stop() throws IOException {
        leaderLatch.close();
    }

    public boolean hasLeadership() {
        return leaderLatch.hasLeadership();
    }
}
