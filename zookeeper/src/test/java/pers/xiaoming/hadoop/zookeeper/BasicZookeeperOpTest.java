package pers.xiaoming.hadoop.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Test;

public class BasicZookeeperOpTest extends ZookeeperTestBase {

    @Test(expected = KeeperException.class)
    public void curd() throws KeeperException, InterruptedException {
        String key = "/zk_demo";
        String value = "value";
        int version = 0;

        // clean up
        Stat stat = zooKeeper.exists(key, false);
        if (stat != null) {
            zooKeeper.delete(key, stat.getVersion());
        }

        // create
        zooKeeper.create(key, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // exist check
        stat = zooKeeper.exists(key, false);
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
