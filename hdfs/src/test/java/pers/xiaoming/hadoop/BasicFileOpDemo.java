package pers.xiaoming.hadoop;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

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
}
