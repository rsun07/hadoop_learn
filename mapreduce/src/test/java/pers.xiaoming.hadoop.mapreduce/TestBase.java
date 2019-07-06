package pers.xiaoming.hadoop.mapreduce;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class TestBase {
    protected static final String ROOT_RESOURCE_PATH = "./src/test/resources";


    protected static void cleanupOutputDir(String path) throws IOException {
        File file = new File(path);
        if (file.isDirectory()) {
            FileUtils.deleteDirectory(file);
        }
    }
}
