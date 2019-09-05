package com.amal.stackOverflow;

import java.util.HashMap;

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
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


public class xmlToAvro extends Configured implements Tool {

  public static final HashMap<String, Schema> SCHEMAS;
  static {
    SCHEMAS = new HashMap<String, Schema>();
    SCHEMAS.put("posts", new Schema.Parser().parse(
        "{\r\n  \"type\":  \"record\",\r\n  \"name\":  \"StackOverflowPostRecord\",\r\n  \"doc\":   \"A Record\",\r\n  \"fields\": [\r\n    { \"name\": \"id\", \"type\": \"string\" },\r\n    { \"name\": \"posttypeid\", \"type\": [\"null\", \"string\"] },\r\n    { \"name\": \"acceptedanswerid\", \"type\": [\"null\", \"string\"] },\r\n    { \"name\": \"parentid\", \"type\": [\"null\", \"string\"] },\r\n    { \"name\": \"creationdate\", \"type\": [\"null\", \"string\"] },\r\n    { \"name\": \"score\", \"type\": [\"null\", \"string\"] },\r\n    { \"name\": \"viewcount\", \"type\": [\"null\", \"string\"] },\r\n    { \"name\": \"body\", \"type\": [\"null\", \"string\"] },\r\n    { \"name\": \"owneruserid\", \"type\": [\"null\", \"string\"] },\r\n    { \"name\": \"ownerdisplayname\", \"type\": [\"null\", \"string\"] },\r\n    { \"name\": \"lasteditoruserid\", \"type\": [\"null\", \"string\"] },\r\n    { \"name\": \"lasteditordisplayname\", \"type\": [\"null\", \"string\"] },\r\n    { \"name\": \"lasteditdate\", \"type\": [\"null\", \"string\"] },\r\n    { \"name\": \"lastactivitydate\", \"type\": [\"null\", \"string\"] },\r\n    { \"name\": \"title\", \"type\": [\"null\", \"string\"] },\r\n    { \"name\": \"tags\", \"type\": [\"null\", \"string\"] },\r\n    { \"name\": \"answercount\", \"type\": [\"null\", \"string\"] },\r\n    { \"name\": \"commentcount\", \"type\": [\"null\", \"string\"] },\r\n    { \"name\": \"favoritecount\", \"type\": [\"null\", \"string\"] },\r\n    { \"name\": \"closeddate\", \"type\": [\"null\", \"string\"] },\r\n    { \"name\": \"communityowneddate\", \"type\": [\"null\", \"string\"] }\r\n  ]\r\n}"
    ));
  }
  
  public int run(String[] args) throws Exception {
    Path inputPath = new Path(args[0]);
    Path outputDir = new Path(args[1]);
    // Create configuration
    Configuration conf = this.getConf(); //new Configuration(true);
    conf.set("xmlinput.start", "<row");
    conf.set("xmlinput.end", "/>");
    
    // Create job
    Job job = Job.getInstance(conf, "StackOverflow XML to Avro");
    job.setJarByClass(getClass());
    
    // Setup MapReduce
    job.setMapperClass(StackOverflowMapper.class);
    job.setNumReduceTasks(0);
    
    AvroMultipleOutputs.addNamedOutput(job, "posts", AvroKeyOutputFormat.class, SCHEMAS.get("posts"), null);
    AvroMultipleOutputs.setCountersEnabled(job, true);
    
    //AvroJob.setMapOutputKeySchema(job, SCHEMAS.get("posts")); 
    //Schema.create(Schema.Type.INT));
    AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
    AvroJob.setMapOutputValueSchema(job, SCHEMAS.get("posts"));
    AvroJob.setOutputKeySchema(job, SCHEMAS.get("posts"));
    
    // Specify key / value
   // job.setOutputKeyClass(Text.class);
   // job.setOutputValueClass(Text.class);

    // Input
    FileInputFormat.addInputPath(job, inputPath);
    job.setInputFormatClass(XmlInputFormat.class);

    // Output
    FileOutputFormat.setOutputPath(job, outputDir);
    FileOutputFormat.setCompressOutput(job, true);
    //job.setOutputFormatClass(AvroKeyOutputFormat.class);
    LazyOutputFormat.setOutputFormatClass(job, AvroKeyOutputFormat.class);
        
    // Delete output if exists
    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(outputDir))
      hdfs.delete(outputDir, true);
        
    // Execute job
    int result = job.waitForCompletion(true) ? 0 : 1;
    return result;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new xmlToAvro(), args);
    System.exit(exitCode);
  }
}
