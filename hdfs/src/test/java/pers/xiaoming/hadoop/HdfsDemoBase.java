package pers.xiaoming.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

public class HdfsDemoBase {
    private static final String DEMO_DEFAULT_FS = "hdfs://localhost:9000";

    protected static final Logger logger = Logger.getLogger(HdfsDemoBase.class);

    protected static FileSystem fs;


    @BeforeClass
    public static void setup() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", DEMO_DEFAULT_FS);
        fs = FileSystem.get(configuration);
    }

    @AfterClass
    public static void close() throws IOException {
        if (fs != null) {
            fs.close();
        }
    }
}
