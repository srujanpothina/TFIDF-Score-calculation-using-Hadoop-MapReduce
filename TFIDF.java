// Srujan Pothina
// 800965600
//spothina@uncc.edu
package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.fs.RemoteIterator;
import java.io.File;
import java.lang.Math;
import java.util.Collection;;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.conf.Configuration;


public class TFIDF extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TFIDF.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new TFIDF(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job1 = Job.getInstance(getConf(), "TermFrequency");
		job1.setJarByClass(this.getClass());
		// Use TextInputFormat, the default unless job.setInputFormatClass is used
		// Routine to get number of input files from the path
		int count = 0;
		FileSystem fs = FileSystem.get(getConf());
		boolean recursive = false;
		RemoteIterator<LocatedFileStatus> ri = fs.listFiles(new Path(args[0]), recursive);
		while (ri.hasNext()){
		    count++;
		    ri.next();
		}
		//sending #files parameter to reducer by setting a parameter to the configuration
		getConf().set("noOfFiles",String.valueOf(count));
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		//return job1.waitForCompletion(true) ? 0 : 1;
		// checking if job1 is completed and invoking job 2 with output of job1
		boolean success = job1.waitForCompletion(true);
		Job job2 = Job.getInstance(getConf(), "TFIDF");
		
		if (success) {
		job2.setJarByClass(this.getClass());
		//Taking output of job1
		FileInputFormat.addInputPaths(job2, args[1]+"/part-r-00000");
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);
		//job2.setInputFormatClass(TextInputFormat.class);
		//job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		//success = job2.waitForCompletion(true);
		}

		return job2.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map1 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		private final static DoubleWritable one = new DoubleWritable(1);
		private Text word = new Text();
		private long numRecords = 0;    
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line = lineText.toString();
			Text currentWord = new Text();

			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String filename = fileSplit.getPath().getName();

			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				//Appending word and file name
				currentWord = new Text(word.toLowerCase()+"#####"+filename);
				context.write(currentWord,one);
			}
		}
	}


	public static class Reduce1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text word, Iterable<DoubleWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (DoubleWritable count : counts) {
				sum += count.get();
			}
			// Calculating the logarithimic 
			double logTF = 1 + Math.log10(sum);
			context.write(word, new DoubleWritable(logTF));
		}
	}
	// Map2 and Reduce2 are for second job instance
	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
		
		private final static DoubleWritable one = new DoubleWritable(1);
		private Text word = new Text();

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			// Reading Term frequence line by line
			String line = lineText.toString();
			//splitting life to seperate "term" and "filename termfrequency"
			String[] term = line.split("#####");
			String removeSpaces = term[1].trim().replaceAll("\\s+",",");
			// splitting "filename termfrequency"
			String[] doc_tf= removeSpaces.split(",");
			// Converting file name to type Text
			word = new Text(term[0]);
			Text val = new Text();
			// Appending filename and its term frequency in the form a=1.222
			val = new Text(doc_tf[0] + "=" + doc_tf[1]);
			// sending the term and filename=termfrequence to the reducer
			context.write(word, val);
		}
	}

	public static class Reduce2 extends Reducer<Text, Text, Text, DoubleWritable> {

		// taking input in key as Text and value as postings list in form Iterable<Text>
		public void reduce(Text term, Iterable<Text> files, Context context)
				throws IOException, InterruptedException {
			DoubleWritable one = new DoubleWritable(); 
			
			// code to get the number of files set in configuration to use for idf calculation
			Configuration conf = context.getConfiguration();
			int noOfFiles = Integer.parseInt(conf.get("noOfFiles"));
			int size = 0;
			
			List<String> postingList = new ArrayList<String>();
			// code to get postings list size for a term
			for(Text a : files){
				size++;
				// As iterable loses its trace after iterating once I am storing it in an ArrayList<String>
				postingList.add(a.toString());
			}
			Text newTerm = new Text();
			
			// Routine for computing idf value and then tfidfvalue for each term,doc combination
			for (String posting : postingList) {
				
				//splitting each single posting from a list
				String[] fileTf = posting.split("=");
				// Getting key ready as term#####fileName
				newTerm = new Text(term.toString()+"#####"+fileTf[0]);
				// Computing idf
				String files_no = String.valueOf(noOfFiles);
				String sizeStr = String.valueOf(size);
				double idf = Math.log10(1.0 + (Double.parseDouble(files_no)/Double.parseDouble(sizeStr)));
				// Computing tf-idf
				double tf_idf= idf * Double.parseDouble(fileTf[1]);
				// Converting it to doublewritable
				DoubleWritable tfidf = new DoubleWritable(tf_idf); 
				// Writing the term#####fileName tf-idf to output file
				context.write(newTerm, tfidf);
			}

		}
	}
}
