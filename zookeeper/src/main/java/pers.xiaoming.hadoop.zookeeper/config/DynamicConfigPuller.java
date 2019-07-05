package pers.xiaoming.hadoop.zookeeper.config;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DynamicConfigPuller implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(DynamicConfigPuller.class);
    private final ZooKeeper zooKeeper;
    private Map<String, String> configs;

    private Watcher configUpdateWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeDataChanged) {
                configUpdate(event.getPath());
            }
        }
    };

    public DynamicConfigPuller(ZooKeeper zooKeeper, String[] keys) throws KeeperException, InterruptedException {
        this.zooKeeper = zooKeeper;
        initalConfigs(keys);
    }

    private void initalConfigs(String[] keys) throws KeeperException, InterruptedException {
        this.configs = new ConcurrentHashMap<>();
        for (String key : keys) {
            byte[] data = zooKeeper.getData(key, configUpdateWatcher, null);
            configs.put(key, new String(data));
        }
    }

    private void configUpdate(String key) {
        String value = null;
        try {
            byte[] data = zooKeeper.getData(key, false, null);
            value = new String(data);
        } catch (KeeperException e) {
            logger.error("Zookeeper data not found for: " + key);
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage());
        }
        configs.put(key, value);
    }

    public String get(String key) {
        return configs.get(key);
    }

    @Override
    public void close() throws Exception {
        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }
}
