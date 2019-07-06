package pers.xiaoming.hadoop.mapreduce.payment;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import pers.xiaoming.hadoop.mapreduce.TestBase;

import java.io.IOException;

public class PartitionerTest extends TestBase {
    private static final String RESOURCES_PATH = ROOT_RESOURCE_PATH + "/payment";
    private static final String FIVE_K_INPUT_PATH = RESOURCES_PATH + "/payment_input_5k";
    private static final String FIVE_K_OUTPUT_PATH = RESOURCES_PATH + "/partitioner_output_5k";

    @BeforeClass
    public static void cleanup() throws IOException {
        cleanupOutputDir(FIVE_K_OUTPUT_PATH);
    }

    @Test
    public void testOnePartition() throws IOException, ClassNotFoundException, InterruptedException {
        PaymentPartitionCountDriver driver = new PaymentPartitionCountDriver(FIVE_K_INPUT_PATH, FIVE_K_OUTPUT_PATH);
        Assert.assertTrue(driver.run());
    }

    @Test
    public void testThreePartition() throws IOException, ClassNotFoundException, InterruptedException {
        // partition number is bigger than 1 and lesser than num of partition
        // exception will throw
        PaymentPartitionCountDriver driver = new PaymentPartitionCountDriver(FIVE_K_INPUT_PATH, FIVE_K_OUTPUT_PATH, 3);
        Assert.assertFalse(driver.run());
    }

    @Test
    public void testFivePartition() throws IOException, ClassNotFoundException, InterruptedException {
        PaymentPartitionCountDriver driver = new PaymentPartitionCountDriver(FIVE_K_INPUT_PATH, FIVE_K_OUTPUT_PATH, 5);
        Assert.assertTrue(driver.run());
    }
}
