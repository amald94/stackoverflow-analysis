package com.amal.stackOverflow;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class StackOverflowMapper extends Mapper<LongWritable, Text, AvroKey<GenericRecord>, NullWritable> {
  
  protected GenericRecord record;
  protected String amos_name;
  
  public AvroMultipleOutputs amos;
  
  @Override
  public void setup(Context context) {
    amos = new AvroMultipleOutputs(context);
    InputSplit split = context.getInputSplit();
    String fileName = ((FileSplit) split).getPath().getName().toLowerCase().split("\\.")[0];
    if (xmlToAvro.SCHEMAS.containsKey(fileName)) {
      record = new GenericData.Record(xmlToAvro.SCHEMAS.get(fileName));
      amos_name = fileName;
    }
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    amos.close();
  }
  
  @Override
  public void map(LongWritable key, Text value, Context context) 
      throws IOException, InterruptedException {
    if (amos_name == null) {
      // very crude and inefficient way to skip files with no schemas.
      return;
    }
    try {
      extractValues(value.toString(), record);
      amos.write(amos_name, new AvroKey<GenericRecord>(record));
    } catch(AvroRuntimeException e) {
      System.out.println(e);
    }
    // Using multiple outputs, so no write to context is required or wanted
  }
  
  public void extractValues(String str, GenericRecord rec) {
    // Extracting XML values using a regex is not recommended. In this case
    // (ie an POC project) it seems a sensible easy way to get started.
    Pattern regex = Pattern.compile("\\s([^\\s]+)=\"([^\"]*)\""); 
    Matcher matcher = regex.matcher(str);
    while (matcher.find()) {
      // Making the assumption that the XML does not contain any attribute names that are
      // not in the schema for the record.
      record.put(matcher.group(1).toLowerCase(),
          // HTML escaper does not escape &quot, but XML one does but handles little else, so using both :-/
          StringEscapeUtils.unescapeXml(nullAsEmpty(matcher.group(2))));
    }
  }
  
  private String nullAsEmpty(String str) {
    if (str == null) {
      return "";
    } else {
      return str;
    }
  }
}
