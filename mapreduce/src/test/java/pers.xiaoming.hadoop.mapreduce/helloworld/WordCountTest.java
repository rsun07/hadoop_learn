package pers.xiaoming.hadoop.mapreduce.helloworld;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import pers.xiaoming.hadoop.mapreduce.TestBase;

import java.io.IOException;

public class WordCountTest extends TestBase {
    private static final String RESOURCES_PATH = ROOT_RESOURCE_PATH + "/word_count";
    private static final String INPUT_PATH = RESOURCES_PATH + "/wc_input";
    private static final String OUTPUT_PATH = RESOURCES_PATH + "/wc_output";


    @BeforeClass
    public static void cleanup() throws IOException {
        cleanupOutputDir(OUTPUT_PATH);
    }

    @Test
    public void test() throws IOException, ClassNotFoundException, InterruptedException {
        WordCountDriver driver = new WordCountDriver(INPUT_PATH, OUTPUT_PATH);
        Assert.assertTrue(driver.run());
    }
}
