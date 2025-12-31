package com.test;

import com.alibaba.datax.example.ExampleContainer;
import com.alibaba.datax.example.util.PathUtil;
import org.junit.Test;

/**
 * {@code Author} FuYouJ
 * {@code Date} 2023/8/14 20:16
 */

public class StreamReader2StreamWriterTest {
    @Test
    public void testStreamReader2StreamWriter() {
        //String path = "/mysql2pg/mysql2pg.json";
        String path = "/pg2mysql/pg2mysql.json";
        //String path = "/mysqldebezumi2stream4.json";
        String jobPath = PathUtil.getAbsolutePathFromClassPath(path);
        ExampleContainer.start(jobPath);
    }
}
