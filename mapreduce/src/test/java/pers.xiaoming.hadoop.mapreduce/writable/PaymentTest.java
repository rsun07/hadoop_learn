package pers.xiaoming.hadoop.mapreduce.writable;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class PaymentTest {
    private static final String RESOURCES_PATH = "./src/test/resources/payment";
    private static final String SHORT_INPUT_PATH = RESOURCES_PATH + "/payment_input_short_test";
    private static final String SHORT_OUTPUT_PATH = RESOURCES_PATH + "/payment_output_short_test";
    private static final String FIVE_K_INPUT_PATH = RESOURCES_PATH + "/payment_input_5k";
    private static final String FIVE_K_OUTPUT_PATH = RESOURCES_PATH + "/payment_output_5k";

    @BeforeClass
    public static void cleanup() throws IOException {
        deleteOutputDir(SHORT_OUTPUT_PATH);
        deleteOutputDir(FIVE_K_OUTPUT_PATH);
    }

    private static void deleteOutputDir(String dirPath) throws IOException {
        File file = new File(dirPath);
        if (file.isDirectory()) {
            FileUtils.deleteDirectory(file);
        }
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
