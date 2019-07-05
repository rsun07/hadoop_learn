package pers.xiaoming.hadoop.zookeeper.leader;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

public class LeaderSelectorServer extends LeaderSelectorListenerAdapter implements LeaderServer {
    private static final Logger logger = Logger.getLogger(LeaderSelectorServer.class);

    private final String name;

    // Abstraction to select a "leader" amongst multiple contenders in a group of JMVs connected
    //  to a Zookeeper cluster.
    private LeaderSelector leaderSelector;

    private final AtomicInteger leaderCount;

    private boolean shouldLead;

    public LeaderSelectorServer(String name, String path, CuratorFramework client) {
        this.name = name;
        this.leaderCount = new AtomicInteger();
        leaderSelector = new LeaderSelector(client, path, this);
        leaderSelector.autoRequeue();
        leaderSelector.start();
    }

    public void stopLead() {
        this.shouldLead = false;
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        this.shouldLead = true;
        logger.info(String.format("Server %s is now the leader, it has been lead for %s times", name, leaderCount.get()));
        while (shouldLead) {
            // check every second
            Thread.sleep(1000);
        }
        logger.info(String.format("Server %s is no longer the leader", name));
    }

    public String getName() {
        return name;
    }

    public boolean hasLeadership() {
        return leaderSelector.hasLeadership();
    }

    public void stop() {
        leaderSelector.close();
    }
}
