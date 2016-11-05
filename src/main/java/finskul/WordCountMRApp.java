package finskul;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import finskul.mr.WordCountMapper;
import finskul.mr.WordCountPartitioner;
import finskul.mr.WordCountReducer;

public class WordCountMRApp extends Configured implements Tool{

	public static enum WORDLENGTH{
		EVENCHAR,
		ODDCHAR
	};
	
	public static void main(String args[])
	{
		try {
			WordCountMRApp driver = new WordCountMRApp();
			 int exitCode = ToolRunner.run(driver, args);
			 System.exit(exitCode);
		}catch (Exception e) {
		
			e.printStackTrace();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String hdfsPath = "hdfs://" + "bonsai-master" + ":" + "9000";
		conf.set("fs.default.name", hdfsPath);		
		conf.set("yarn.resourcemanager.address", "bonsai-master:8032");
		conf.set("mapreduce.jobhistory.address", "bonsai-master:10020");		
		conf.set("mapreduce.framework.name", "yarn");

	    Job job;
		try {
			job = Job.getInstance(conf, "word count");			
			job.setJarByClass(WordCountMRApp.class);
		    job.setMapperClass(WordCountMapper.class);
		    job.setCombinerClass(WordCountReducer.class);
		    job.setReducerClass(WordCountReducer.class);
		    job.setPartitionerClass(WordCountPartitioner.class);
		    job.setNumReduceTasks(2);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path("/user/jpvel/finskul/curation.final.croma.txt"));
		    FileOutputFormat.setOutputPath(job, new Path("/user/jpvel/finskul/partitioner-mr-out"));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (IOException e) {
			throw new Exception(e);
		} catch (ClassNotFoundException e) {
			throw new Exception(e);
		} catch (InterruptedException e) {
			throw new Exception(e);
		}
		
		
		return 0;
	}
	public static int evenOrOdd(Text key) {
		int length = key.getLength();
		if(length%2==0)
			return 0;
		else
			return 1;
	}
}
