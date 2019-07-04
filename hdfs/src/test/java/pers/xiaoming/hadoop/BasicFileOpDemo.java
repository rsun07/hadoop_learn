package pers.xiaoming.hadoop;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class BasicFileOpDemo extends HdfsDemoBase {
    @Test
    public void upload() throws IOException {
        fs.copyFromLocalFile(new Path(LOCAL_UPLOAD_FILE_PATH), new Path(HDFS_UPLOAD_FILE_PATH));
    }

    @Test
    public void download() throws IOException {
        File file = new File(LOCAL_DOWNLOAD_FILE_PATH);
        file.deleteOnExit();

        fs.copyToLocalFile(new Path(HDFS_DOWNLOAD_FILE_PATH), new Path(LOCAL_DOWNLOAD_FILE_PATH));
    }

    @Test
    public void iterateDir() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path(HDFS_DEMO_ROOT), true);

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

    @Test
    public void listDir() throws IOException {
        listDir(HDFS_DEMO_ROOT, "--");
    }

    private void listDir(String dirPath, String indent) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path(dirPath));

        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isDirectory()) {
                logger.info(indent + "d:" + fileStatus.getPath().getName());
                String path = fileStatus.getPath().getParent() + "/" + fileStatus.getPath().getName();
                listDir(path, indent + "--");
            } else if (fileStatus.isFile()) {
                logger.info(indent + "f:" + fileStatus.getPath().getName());
            }
        }
    }
}
