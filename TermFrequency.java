// Srujan Pothina
// 800965600
//spothina@uncc.edu
package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.File;
import java.lang.Math;

import org.apache.log4j.Logger;

public class TermFrequency extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(TermFrequency.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new TermFrequency(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "TermFrequency");
    job.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private final static DoubleWritable one = new DoubleWritable(1);
    private Text word = new Text();
    private long numRecords = 0;    
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
    @Override
    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      Text currentWord = new Text();
      
      //splitting files to get each file name
      FileSplit split = (FileSplit)context.getInputSplit();
      String filename = split.getPath().getName();
      
      for (String word : WORD_BOUNDARY.split(line)) {
        if (word.isEmpty()) {
            continue;
        }
        //appending filename and term
            currentWord = new Text(word.toLowerCase()+"#####"+filename);
            context.write(currentWord,one);
        }
    }
  }

  public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    public void reduce(Text word, Iterable<DoubleWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (DoubleWritable count : counts) {
        sum += count.get();
      }
      
      // computing logarithimic term frequency value
      double logTF = 1 + Math.log10(sum);
      
      // Writing term and term frequency to the term
      context.write(word, new DoubleWritable(logTF));
    }
  }
}
