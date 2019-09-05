package com.amal.Hadoop.cache;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

public class DistCacheMapperTest   {

    MapDriver<LongWritable, Text, NullWritable, Text> mapDriver;
    private static final Logger log = Logger.getLogger(DistCacheMapper.class);
    
    @Before
    public void setup() throws URISyntaxException {
        DistCacheMapper mapper = new DistCacheMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        
        // Notice how to add a cachefile to the mapper for testing
        mapDriver.addCacheFile("./input/data.csv");
        mapDriver.addCacheFile(new URI("./input.data.csv#lookup.csv"));
    }
    
    
    @Test
    public void splitValidRecordIntoTokens() throws IOException, InterruptedException {
        log.info("running the test");
        List<Text> values = new ArrayList<Text>();
        values.add(new Text("abc"));
        mapDriver.withInput(new LongWritable(1), new Text("abcdef"))
        .withOutput(NullWritable.get(), new Text("abcdef"))
        .runTest();
    } 
    
    
}