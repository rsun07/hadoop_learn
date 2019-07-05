package pers.xiaoming.hadoop.zookeeper.leader;

import java.io.IOException;

public interface LeaderServer {
    String getName();

    boolean hasLeadership();

    void stop() throws IOException;
}
