package pers.xiaoming.hadoop.mapreduce.bid.max;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import pers.xiaoming.hadoop.mapreduce.TestBase;

import java.io.IOException;

public class BidTopTwoTest extends TestBase {
    private static final String RESOURCES_PATH = ROOT_RESOURCE_PATH + "/bid";
    private static final String INPUT_PATH = RESOURCES_PATH + "/bid_input";
    private static final String OUTPUT_PATH= RESOURCES_PATH + "/bid_top2_output";

    @BeforeClass
    public static void cleanup() throws IOException {
        cleanupOutputDir(OUTPUT_PATH);
    }

    @Test
    public void test() throws IOException, ClassNotFoundException, InterruptedException {
        BidTopTwoDriver driver = new BidTopTwoDriver(INPUT_PATH, OUTPUT_PATH);
        Assert.assertTrue(driver.run());
    }
}
