package pers.xiaoming.hadoop;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class BasicFileOpDemo extends HdfsDemoBase {
    @Test
    public void upload() throws IOException {
        fs.copyFromLocalFile(new Path("./src/test/resources/upload_demo"), new Path("/demo/upload_demo"));
    }

    @Test
    public void download() throws IOException {
        String targetFilePath = "./src/test/resources/download_demo";

        File file = new File(targetFilePath);
        file.deleteOnExit();

        fs.copyToLocalFile(new Path("/demo/download_demo"), new Path(targetFilePath));
    }

    @Test
    public void iterateDir() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/demo"), true);

        while(listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();
            BlockLocation[] blockLocations = status.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();

                logger.info(String.format("Path name : %s\t, lenth : %s\t, permission : %s\t, group : %s\t, hosts : %s\t",
                        status.getPath().getName(), status.getLen(), status.getPermission(), status.getGroup(), Lists.newArrayList(hosts)));
            }
        }
    }
}
