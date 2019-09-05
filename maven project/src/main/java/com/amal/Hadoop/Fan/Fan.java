package com.amal.Hadoop.Fan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Fan extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);

        // Create configuration
        Configuration conf = new Configuration(true);

        // Create job
        Job job = Job.getInstance(conf, "Fan");
        job.setJarByClass(getClass());

        // Setup MapReduce
        job.setMapperClass(FanOutMapper.class);
        job.setNumReduceTasks(0);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, outputDir);
        //
        // Changing the outputFormatClass by commenting the next line
        // and adding the following one, prevents a zero byte file from
        // being created when you use multi-outputs
        //
        job.setOutputFormatClass(TextOutputFormat.class);
        //LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        //
        // If you want to have named outputs, then define them upfront here
        //
        //  MultipleOutputs.addNamedOutput(job, "badRecords", TextOutputFormat.class,
        //          NullWritable.class, Text.class);
        //  MultipleOutputs.addNamedOutput(job, "goodRecords", TextOutputFormat.class,
        //          NullWritable.class, Text.class);
        
        // Delete output if exists
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);

        // Execute job
        int result = job.waitForCompletion(true) ? 0 : 1;
        
        // Access a single counter by name - remember it can be null if it was never incremented
        Counters counters = job.getCounters();
        System.out.println("******"+counters.findCounter("com.amal.Hadoop.Fan.OtherCounters", "GOOD_RECORDS").getValue());
        
        // Example code to iterate through all the counters, including custom counters     
        for (CounterGroup group : counters) {
            System.out.println("* Counter Group: " + group.getDisplayName() + " (" + group.getName() + ")");
            System.out.println("  number of counters in this group: " + group.size());
            for (Counter counter : group) {
              System.out.println("  - " + counter.getDisplayName() + ": " + counter.getName() + ": "+counter.getValue());
            }
          }
        
        return result;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Fan(), args);
        System.exit(exitCode);
    }
}