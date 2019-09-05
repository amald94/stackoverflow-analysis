package com.amal.Hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver; 
import org.apache.hadoop.mapred.Utils.OutputFileUtils.OutputFilesFilter;
import org.junit.*;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class WordCountTest {

    @Test
    public void test() throws Exception {
        Configuration conf = new Configuration(); 
        conf.set("fs.defaultFS", "file:///"); 
        conf.set("mapred.framework.name", "local");
        Path input = new Path("input"); 
        Path output = new Path("output");

        FileSystem fs = FileSystem.getLocal(conf); 
        fs.delete(output, true); // delete old output
        WordCount driver = new WordCount();
        driver.setConf(conf);
        int exitCode = driver.run(new String[] { input.toString(), output.toString() });
        assertThat(exitCode, is(0));
        checkOutput(conf, output);
    }
    
    private void checkOutput(Configuration conf, Path output) throws IOException {
        FileSystem fs = FileSystem.getLocal(conf);
        Path[] outputFiles = FileUtil.stat2Paths(
        fs.listStatus(output, new OutputFilesFilter()));
        assertThat(outputFiles.length, is(1));
        BufferedReader actual = asBufferedReader(fs.open(outputFiles[0]));
        BufferedReader expected = asBufferedReader(getClass().getResourceAsStream("/expected.txt"));
        String expectedLine;
        while ((expectedLine = expected.readLine()) != null) {
            String actualLine = actual.readLine();
            assertThat(actualLine, is(expectedLine));
        }
        assertThat(actual.readLine(), nullValue());
        actual.close();
        expected.close();
    }
        
    private BufferedReader asBufferedReader(InputStream in) throws IOException {
        return new BufferedReader(new InputStreamReader(in));
    }  
        
}
