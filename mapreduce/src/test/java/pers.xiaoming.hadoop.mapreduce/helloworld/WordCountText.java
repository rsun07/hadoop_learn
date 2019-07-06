package pers.xiaoming.hadoop.mapreduce.helloworld;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class WordCountText {
    private static final String RESOURCES_PATH = "./src/test/resources";
    private static final String INPUT_PATH = RESOURCES_PATH + "/wc_input";
    private static final String OUTPUT_PATH = RESOURCES_PATH + "/wc_output";


    @BeforeClass
    public static void cleanup() throws IOException {
        File file = new File(OUTPUT_PATH);
        if (file.isDirectory()) {
            FileUtils.deleteDirectory(file);
        }
    }

    @Test
    public void test() throws IOException, ClassNotFoundException, InterruptedException {
        WordCountDriver driver = new WordCountDriver(INPUT_PATH, OUTPUT_PATH);
        driver.run();
    }
}
