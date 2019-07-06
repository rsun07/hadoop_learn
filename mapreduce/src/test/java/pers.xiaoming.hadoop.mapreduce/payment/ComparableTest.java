package pers.xiaoming.hadoop.mapreduce.payment;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import pers.xiaoming.hadoop.mapreduce.TestBase;

import java.io.IOException;

public class ComparableTest extends TestBase {
    private static final String RESOURCES_PATH = ROOT_RESOURCE_PATH + "/payment";
    private static final String INPUT_PATH = RESOURCES_PATH + "/payment_input_5k";
    private static final String OUTPUT_PATH_PREFIX = RESOURCES_PATH + "/comparable_output_5k";
    private static final String NUM_PARTITION_ONE_OUTPUT_PATH = OUTPUT_PATH_PREFIX + "_num_p_1";
    private static final String NUM_PARTITION_FIVE_OUTPUT_PATH = OUTPUT_PATH_PREFIX + "_num_p_5";

    @BeforeClass
    public static void cleanup() throws IOException {
        cleanupOutputDir(NUM_PARTITION_ONE_OUTPUT_PATH);
        cleanupOutputDir(NUM_PARTITION_FIVE_OUTPUT_PATH);
    }

    @Test
    public void testOnePartition() throws IOException, ClassNotFoundException, InterruptedException {
        PaymentPartitionComparableCountDriver driver = new PaymentPartitionComparableCountDriver(INPUT_PATH, NUM_PARTITION_ONE_OUTPUT_PATH);
        Assert.assertTrue(driver.run());
    }

    @Test
    public void testFivePartition() throws IOException, ClassNotFoundException, InterruptedException {
        PaymentPartitionComparableCountDriver driver = new PaymentPartitionComparableCountDriver(INPUT_PATH, NUM_PARTITION_FIVE_OUTPUT_PATH, 5);
        Assert.assertTrue(driver.run());
    }
}
