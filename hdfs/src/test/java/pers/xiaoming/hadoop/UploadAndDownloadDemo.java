package pers.xiaoming.hadoop;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

public class UploadAndDownloadDemo extends HdfsDemoBase {


    @Test
    public void upload() throws IOException {
        fs.copyFromLocalFile(new Path("./src/test/resources/upload_demo"), new Path("/demo/upload_demo"));
    }
}
