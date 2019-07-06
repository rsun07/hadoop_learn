package pers.xiaoming.hadoop.mapreduce.input_output_format.n_line_text_input_format;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import pers.xiaoming.hadoop.mapreduce.TestBase;
import pers.xiaoming.hadoop.mapreduce.input_output_format.n_line_input_format.NLineTextDriver;

import java.io.IOException;

public class NLineTextTest extends TestBase {
    private static final String RESOURCES_PATH = ROOT_RESOURCE_PATH + "/input_output_format";
    private static final String INPUT_PATH = RESOURCES_PATH + "/nline_input";
    private static final String OUTPUT_PATH = RESOURCES_PATH + "/nline_output";

    @BeforeClass
    public static void cleanup() throws IOException {
        cleanupOutputDir(OUTPUT_PATH);
    }

    @Test
    public void test() throws IOException, ClassNotFoundException, InterruptedException {
        NLineTextDriver driver = new NLineTextDriver(INPUT_PATH, OUTPUT_PATH);
        Assert.assertTrue(driver.run());
    }
}
