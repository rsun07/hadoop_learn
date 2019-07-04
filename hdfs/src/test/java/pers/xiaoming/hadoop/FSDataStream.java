package pers.xiaoming.hadoop;

import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

public class FSDataStream extends HdfsDemoBase {
    @Test
    public void upload() throws FileNotFoundException {
        FileInputStream inputStream = new FileInputStream(new File(LOCAL_UPLOAD_FILE_PATH));
    }
}
