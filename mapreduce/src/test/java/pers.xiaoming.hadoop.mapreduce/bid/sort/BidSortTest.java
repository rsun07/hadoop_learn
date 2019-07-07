package pers.xiaoming.hadoop.mapreduce.bid.sort;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import pers.xiaoming.hadoop.mapreduce.TestBase;
import pers.xiaoming.hadoop.mapreduce.bid.sort.BidSortDriver;

import java.io.IOException;

// This is the right version of sort result based on WritableComparable bean's compare method
// The result is sorted by the given order of WritableComparable's compare method
public class BidSortTest extends TestBase {
    private static final String RESOURCES_PATH = ROOT_RESOURCE_PATH + "/bid";
    private static final String INPUT_PATH = RESOURCES_PATH + "/bid_input";
    private static final String OUTPUT_PATH_PREFIX = RESOURCES_PATH + "/bid_sort_output_5k";
    private static final String NUM_PARTITION_ONE_OUTPUT_PATH = OUTPUT_PATH_PREFIX + "_num_p_1";
    private static final String NUM_PARTITION_FIVE_OUTPUT_PATH = OUTPUT_PATH_PREFIX + "_num_p_5";

    @BeforeClass
    public static void cleanup() throws IOException {
        cleanupOutputDir(NUM_PARTITION_ONE_OUTPUT_PATH);
        cleanupOutputDir(NUM_PARTITION_FIVE_OUTPUT_PATH);
    }

    @Test
    public void testOnePartition() throws IOException, ClassNotFoundException, InterruptedException {
        BidSortDriver driver = new BidSortDriver(INPUT_PATH, NUM_PARTITION_ONE_OUTPUT_PATH);
        Assert.assertTrue(driver.run());
    }

    @Test
    public void testFivePartition() throws IOException, ClassNotFoundException, InterruptedException {
        BidSortDriver driver = new BidSortDriver(INPUT_PATH, NUM_PARTITION_FIVE_OUTPUT_PATH, 3);
        Assert.assertTrue(driver.run());
    }
}
