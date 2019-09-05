package com.amal.Hadoop.Fan;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class FanOutMapper extends
    Mapper<LongWritable, Text, NullWritable, Text> {
    
    private MultipleOutputs<NullWritable, Text> mos;
    
    @Override
    public void setup(Context context) {
        mos = new MultipleOutputs<NullWritable, Text>(context);
    }           
    
    // 
    // You must override the cleanup method and close the multi-output object
    // or things do not work correctly.
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
    
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Throw away totally blank lines
        if (value.equals(new Text(""))) {
            return;
        }
        
        // Example of how to increment a custom counter
        context.getCounter(OtherCounters.GOOD_RECORDS).increment(1);

        // Fan the records out into a file that has the first character of the 
        // string as the filename.
        // You can also use named outputs (defined in the job runner class)
        // instead of deriving the filename based on the input lines.
        // If you pass a path with / characters in it, the data will go into subdirs
        // eg 20150304/data etc
        String keyChar = value.toString().substring(0,1).toLowerCase();
        
        // In this example, the keyChar string indicates the filename the data is written
        // into. You can write the same data to many files, and the filename can 
        // contain slashes to make it into a path. The path is relative to the output dir
        // setup in the job config.
        mos.write(NullWritable.get(), value, keyChar);
        // mos.write("goodRecords", NullWritable.get(),value);
        // context.write(NullWritable.get(), value);
    }
}