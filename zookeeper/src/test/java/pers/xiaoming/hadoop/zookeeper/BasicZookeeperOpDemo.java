package pers.xiaoming.hadoop.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class BasicZookeeperOpDemo {
    private static final String CONNECT_STRING = "localhost:2181";
    private static final int SESSION_TIMEOUT = 2000;

    private static final Logger logger = Logger.getLogger(BasicZookeeperOpDemo.class);

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

    @Test(expected = KeeperException.class)
    public void curd() throws KeeperException, InterruptedException {
        String key = "/zk_demo";
        String value = "value";
        int version = 0;

        // clean up
        zooKeeper.delete(key, version);

        // create
        zooKeeper.create(key, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // exist check
        Stat stat = zooKeeper.exists(key, false);
        Assert.assertNotNull(stat);
        logger.info(stat.toString());

        Stat zkReturnedStat = new Stat();
        Assert.assertNotEquals(stat, zkReturnedStat);
        byte[] zkReturnedValue = zooKeeper.getData(key, false, zkReturnedStat);

        Assert.assertArrayEquals(value.getBytes(), zkReturnedValue);
        Assert.assertEquals(stat, zkReturnedStat);

        // delete
        zooKeeper.delete(key, version);

        stat = zooKeeper.exists(key, false);
        Assert.assertNull(stat);

        try {
            zkReturnedValue = zooKeeper.getData(key, false, zkReturnedStat);
        } catch (KeeperException e) {
            logger.error(e.getMessage());
            throw e;
        }
    }
}
