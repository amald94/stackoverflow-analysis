package com.amal.Hadoop.cache;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.amal.Hadoop.Fan.Fan;
import com.amal.Hadoop.Fan.FanOutMapper;

public class DistCache extends Configured implements Tool  {

    public int run(String[] args) throws Exception, IllegalArgumentException {
        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);

        // Create configuration
        Configuration conf = new Configuration(true);

        // Create job
        Job job = Job.getInstance(conf, "DistCache");
        job.setJarByClass(getClass());
        
        //    job.addCacheFile(uri);
        //
        // Where Uri can take a path like /path/to/file#identifier - the identifier
        // part can be used to retrieve the file in the mapper
        //    File some_file = new File("./identifier");
        // I have not tested this method out.
        // To add a cache file, use the code below. Note that the file needs to be in HDFS
        // but it can be a local file if running in local mode
        job.addCacheFile(new Path("./input/data.csv").toUri());
        
        // I think you are supposed to be able to add a file with a #identifier on the end of it:
        // job.addCacheFile(new URI("./input/data.csv#lookup.csv"));
        // And then access the file with ./identifier in the mapper / reducer, but I cannot get it to
        // work.
        
        // Setup MapReduce
        job.setMapperClass(DistCacheMapper.class);
        job.setNumReduceTasks(0);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, outputDir);

        // Delete output if exists
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);

        // Execute job
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Fan(), args);
        System.exit(exitCode);
    }
    
}
