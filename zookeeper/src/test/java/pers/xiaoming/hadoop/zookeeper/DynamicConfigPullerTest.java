package pers.xiaoming.hadoop.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DynamicConfigPullerTest extends ZookeeperTestBase {
    private static DynamicConfigPuller configPuller;
    private static String[] testPaths = new String[]{"/k1", "/k2", "/k3"};
    private static String[] testInitValues = new String[]{"v1", "v2", "v3"};
    private static String[] testUpdatedValues = new String[]{"v1_u", "v2_u", "v3_u"};

    @BeforeClass
    public static void configPullerSetup() throws KeeperException, InterruptedException {
        for (int i = 0; i < testPaths.length; i++) {
            String path = testPaths[i];
            Stat stat = zooKeeper.exists(path, false);
            if (stat != null) {
                zooKeeper.delete(path, stat.getVersion());
            }

            zooKeeper.create(path, testInitValues[i].getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        configPuller = new DynamicConfigPuller(zooKeeper, testPaths);
    }

    @Test
    public void test() throws KeeperException, InterruptedException {
        // check initial values
        Stat[] stat = initStats();
        checkValues(testInitValues, stat);

        // update values
        for (int i = 0; i < testPaths.length; i++) {
            zooKeeper.setData(testPaths[i], testUpdatedValues[i].getBytes(), stat[i].getVersion());
        }

        // check updated values
        checkValues(testUpdatedValues, stat);
    }

    private void checkValues(String[] values, Stat[] stat) throws KeeperException, InterruptedException {
        for (int i = 0; i < testPaths.length; i++) {
            String path = testPaths[i];
            byte[] data = zooKeeper.getData(path, false, stat[i]);
            Assert.assertEquals(values[i], new String(data));
            Assert.assertEquals(values[i], configPuller.get(path));
        }
    }

    private Stat[] initStats() {
        Stat[] stats = new Stat[testPaths.length];
        for (int i = 0; i < testPaths.length; i++) {
            stats[i] = new Stat();
        }
        return stats;
    }
}
