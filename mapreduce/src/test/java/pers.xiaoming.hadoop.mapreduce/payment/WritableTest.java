package pers.xiaoming.hadoop.mapreduce.payment;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import pers.xiaoming.hadoop.mapreduce.TestBase;

import java.io.IOException;

public class WritableTest extends TestBase {
    private static final String RESOURCES_PATH = ROOT_RESOURCE_PATH + "/payment";
    private static final String SHORT_INPUT_PATH = RESOURCES_PATH + "/payment_input_short_test";
    private static final String SHORT_OUTPUT_PATH = RESOURCES_PATH + "/payment_output_short_test";
    private static final String FIVE_K_INPUT_PATH = RESOURCES_PATH + "/payment_input_5k";
    private static final String FIVE_K_OUTPUT_PATH = RESOURCES_PATH + "/payment_output_5k";

    @BeforeClass
    public static void cleanup() throws IOException {
        cleanupOutputDir(SHORT_OUTPUT_PATH);
        cleanupOutputDir(FIVE_K_OUTPUT_PATH);
    }

    @Test
    public void testShort() throws IOException, ClassNotFoundException, InterruptedException {
        PaymentCountDriver driver = new PaymentCountDriver(SHORT_INPUT_PATH, SHORT_OUTPUT_PATH);
        Assert.assertTrue(driver.run());
    }

    @Test
    public void test5kInput() throws IOException, ClassNotFoundException, InterruptedException {
        PaymentCountDriver driver = new PaymentCountDriver(FIVE_K_INPUT_PATH, FIVE_K_OUTPUT_PATH);
        Assert.assertTrue(driver.run());
    }
}
