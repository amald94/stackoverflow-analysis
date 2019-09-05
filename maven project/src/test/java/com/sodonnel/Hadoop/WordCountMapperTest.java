/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.amal.Hadoop;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver; 
import org.junit.*;

public class WordCountMapperTest {

    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    
    @Before
    public void setup() {
        WordCountMapper mapper = new WordCountMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }
    
    @Test
    public void splitValidRecordIntoTokens() throws IOException, InterruptedException {
        Text value = new Text("the,quick,brown,fox,the");
        mapDriver.withInput(new LongWritable(), value)
                .withOutput(new Text("the"), new IntWritable(1)) 
                .withOutput(new Text("quick"), new IntWritable(1)) 
                .withOutput(new Text("brown"), new IntWritable(1)) 
                .withOutput(new Text("fox"), new IntWritable(1)) 
                .withOutput(new Text("the"), new IntWritable(1)) 
                .runTest();
    } 
    
    @Test
    public void recordWithSingleWordIsValid() throws IOException, InterruptedException {
        Text value = new Text("the");
        mapDriver.withInput(new LongWritable(), value)
                .withOutput(new Text("the"), new IntWritable(1)) 
                .runTest();
    }
    
    @Test
    public void recordWithEmptyLineOutputsNothing() throws IOException, InterruptedException {
        Text value = new Text("");
        // If you don't specify any 'withOutput' lines, then it EXPECTs no output. 
        // If there is output it will fail the test
        mapDriver.withInput(new LongWritable(), value)
                .runTest();
    }
}
