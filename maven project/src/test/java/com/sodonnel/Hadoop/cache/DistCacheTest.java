package com.amal.Hadoop.cache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Utils.OutputFileUtils.OutputFilesFilter;
import org.junit.*;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.io.IOException;

public class DistCacheTest {

    @Test
    public void test() throws Exception {
        Configuration conf = new Configuration(); 
        conf.set("fs.defaultFS", "file:///"); 
        conf.set("mapred.framework.name", "local");
        Path input = new Path("input"); 
        Path output = new Path("output");
        

        FileSystem fs = FileSystem.getLocal(conf); 
        fs.delete(output, true); // delete old output
        DistCache driver = new DistCache();
        driver.setConf(conf);
        // Could potentially pass cache files here on the command line
        int exitCode = driver.run(new String[] { input.toString(), output.toString() });
        assertThat(exitCode, is(0));
        checkOutput(conf, output);
    }
    
    private void checkOutput(Configuration conf, Path output) throws IOException {
        FileSystem fs = FileSystem.getLocal(conf);
        Path[] outputFiles = FileUtil.stat2Paths(fs.listStatus(output, new OutputFilesFilter()));
        assertThat(outputFiles.length, is(1));
    }        
}
