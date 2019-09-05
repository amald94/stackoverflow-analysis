package com.amal.Hadoop.cache;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

public class DistCacheMapper extends
    Mapper<LongWritable, Text, NullWritable, Text> {
    
    private static final Logger log = Logger.getLogger(DistCacheMapper.class);
    
    @Override
    public void setup(Context context)
            throws IOException, InterruptedException {
        // Two ways to access the cacheFiles - either way, you need to know what
        // file you are actually looking for if different files are required for different things.
        
        // First way, context.getCacheFiles() - returns a list of URIs corresponding to the cache files.
        URI[] YarnFiles = context.getCacheFiles();
        log.info(YarnFiles[0].getPath());
                
        BufferedReader reader = new BufferedReader(new FileReader(YarnFiles[0].getPath())); 
        String line;
        log.info("Reading File using first method");
        while ((line = reader.readLine())!= null) {
            log.info(line);    
        }
        reader.close();
        
        // Second way - the files are localized into the default directory of the JVM running the job
        // So you can access them with the full path used to upload them:
        File some_file = new File("./input/data.csv");
        log.info (some_file.toString());
        
        reader = new BufferedReader(new FileReader(some_file)); 
        log.info("Read file using the second method");
        while ((line = reader.readLine())!= null) {
            log.info(line);    
        }
        reader.close();
        
        super.setup(context);
    }    
    
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Throw away totally blank lines
        if (value.equals(new Text(""))) {
            return;
        }

        context.write(NullWritable.get(), value);
    }
}
