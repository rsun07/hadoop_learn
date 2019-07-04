package pers.xiaoming.hadoop;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class FSDataStream extends HdfsDemoBase {
    private static final int bufferSize = 1024;

    @Test
    public void upload() throws IOException {
        FileInputStream fileInputStream = new FileInputStream(new File(LOCAL_UPLOAD_FILE_PATH));

        FSDataOutputStream fsDataOutputStream = fs.create(new Path(HDFS_UPLOAD_FILE_PATH));

        IOUtils.copyBytes(fileInputStream, fsDataOutputStream, bufferSize);

        IOUtils.closeStream(fsDataOutputStream);
        IOUtils.closeStream(fileInputStream);
    }

    @Test
    public void download() throws IOException {
        FSDataInputStream fsDataInputStream = fs.open(new Path(HDFS_DOWNLOAD_FILE_PATH));

        FileOutputStream fileOutputStream = new FileOutputStream(new File(LOCAL_DOWNLOAD_FILE_PATH));

        IOUtils.copyBytes(fsDataInputStream, fileOutputStream, bufferSize);

        IOUtils.closeStream(fileOutputStream);
        IOUtils.closeStream(fsDataInputStream);
    }

    @Test
    public void fileBufferAndSeek() throws IOException {
        FSDataInputStream fsDataInputStream = fs.open(new Path(HDFS_DOWNLOAD_FILE_PATH));

        FileOutputStream fileOutputStream = new FileOutputStream(new File(LOCAL_DOWNLOAD_FILE_PATH));

        byte[] buffer = new byte[bufferSize];

        for (int i = 0; i < bufferSize * 2; i++) {
            fsDataInputStream.read(buffer);
            fileOutputStream.write(buffer);
        }

        fsDataInputStream.close();
        fileOutputStream.close();

        fsDataInputStream = fs.open(new Path(HDFS_DOWNLOAD_FILE_PATH));
        fsDataInputStream.seek(bufferSize * 2);
        fileOutputStream = new FileOutputStream(new File(LOCAL_DOWNLOAD_FILE_PATH));

        while(fsDataInputStream.read(buffer) > 0) {
            fileOutputStream.write(buffer);
        }

        fsDataInputStream.close();
        fileOutputStream.close();
    }
}
