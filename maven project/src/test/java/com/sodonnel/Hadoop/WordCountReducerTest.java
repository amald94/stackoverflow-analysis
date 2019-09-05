package com.amal.Hadoop;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver; 
import org.junit.*;

public class WordCountReducerTest {

    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    
    @Before
    public void setup() {
        WordCountReducer reducer = new WordCountReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }
    
    @Test
    public void splitValidRecordIntoTokens() throws IOException, InterruptedException {
        reduceDriver.withInput(new Text("the"), Arrays.asList(new IntWritable(1), new IntWritable(2)))
        .withOutput(new Text("the"), new IntWritable(3)) 
        .runTest();
    } 
    
}
