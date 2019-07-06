package pers.xiaoming.hadoop.mapreduce.writable;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class PaymentTest {
    private static final String RESOURCES_PATH = "./src/test/resources";
    private static final String INPUT_PATH = RESOURCES_PATH + "/payment_input";
    private static final String OUTPUT_PATH = RESOURCES_PATH + "/payment_output";


    @BeforeClass
    public static void cleanup() throws IOException {
        File file = new File(OUTPUT_PATH);
        if (file.isDirectory()) {
            FileUtils.deleteDirectory(file);
        }
    }

    @Test
    public void test() throws IOException, ClassNotFoundException, InterruptedException {
        PaymentCountDriver driver = new PaymentCountDriver(INPUT_PATH, OUTPUT_PATH);
        Assert.assertTrue(driver.run());
    }
}
