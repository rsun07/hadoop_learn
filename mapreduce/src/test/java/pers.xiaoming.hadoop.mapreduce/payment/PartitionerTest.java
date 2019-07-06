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
    public void test5kInput() throws IOException, ClassNotFoundException, InterruptedException {
        PaymentCountDriver driver = new PaymentCountDriver(FIVE_K_INPUT_PATH, FIVE_K_OUTPUT_PATH);
        Assert.assertTrue(driver.run());
    }
}
