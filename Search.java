// Srujan Pothina
// 800965600
//spothina@uncc.edu
package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.io.File;
import java.lang.Math;
import java.util.Collection;;
import org.apache.log4j.Logger;

public class Search extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(Search.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Search(), args);
		System.exit(res);
	}
	
	
	public int run(String[] args) throws Exception {
		// reading query as first argument from command line
		getConf().set("query",args[0].toLowerCase());
		// setting this as a search value and reading it in mapper
		Job job2 = Job.getInstance(getConf(), "Search");
		job2.setJarByClass(this.getClass());
		// reading input from part-r-00000 file
		FileInputFormat.addInputPath(job2, new Path(args[1]+"/part-r-00000"));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		return job2.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map2 extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		private final static DoubleWritable one = new DoubleWritable(1);
		private Text word = new Text();
		
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			// reading file input line by line
			String line = lineText.toString();
			// splitting query into string array of individual words
			String q = conf.get("query").toString();
			String[] terms= q.split(" ");
			String[] word = line.split("#####");
			// iterating each term and checking if it is in the line of input 
			for(String t: terms){
				if(t.equals(word[0])){
					// if word is found splitting it to get term and filename tf-idf
					String[] term = line.split("#####");
					//Spliting second part of line after ##### to get filename and tf-idf valeus seperately
					String removeSpaces = term[1].trim().replaceAll("\\s+",",");
					String[] doc_tf= removeSpaces.split(",");
					Text val = new Text();
					val = new Text(doc_tf[0]);
					DoubleWritable tf_idf = new DoubleWritable(Double.parseDouble(doc_tf[1]));
					// Sending filename,tf-idf values to reducer 
					context.write(val,tf_idf);
				}
			}

		}
	}
	public static class Reduce2 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<DoubleWritable> counts, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			// Groups each files tf-idf and iterating them
			for (DoubleWritable count : counts) {
				//adding all tf-idfs of a files
				sum += count.get();
			}
			// getting each documents tf-idf value for the respective query given through command line 
			context.write(word, new DoubleWritable(sum));
		}
	}



}
