package pers.xiaoming.hadoop.mapreduce.input_output_format.key_value_text_input_format;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import pers.xiaoming.hadoop.mapreduce.TestBase;

import java.io.IOException;

public class KVTextTest extends TestBase {
    private static final String RESOURCES_PATH = ROOT_RESOURCE_PATH + "/input_output_format";
    private static final String INPUT_PATH = RESOURCES_PATH + "/kv_input";
    private static final String OUTPUT_PATH = RESOURCES_PATH + "/kv_output";

    @BeforeClass
    public static void cleanup() throws IOException {
        cleanupOutputDir(OUTPUT_PATH);
    }

    @Test
    public void test() throws IOException, ClassNotFoundException, InterruptedException {
        KVTextDriver driver = new KVTextDriver(INPUT_PATH, OUTPUT_PATH);
        Assert.assertTrue(driver.run());
    }
}
