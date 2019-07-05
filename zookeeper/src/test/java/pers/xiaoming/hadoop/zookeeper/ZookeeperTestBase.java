package pers.xiaoming.hadoop.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

public class ZookeeperTestBase {
    private static final String CONNECT_STRING = "localhost:2181";
    private static final int SESSION_TIMEOUT = 2000;

    protected static final Logger logger = Logger.getLogger(BasicZookeeperOpTest.class);

    protected static ZooKeeper zooKeeper;

    @BeforeClass
    public static void init() throws IOException {
        zooKeeper = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
            }
        });
    }

    @AfterClass
    public static void close() throws InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }
}
