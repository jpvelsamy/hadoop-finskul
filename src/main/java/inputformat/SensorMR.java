package inputformat;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SensorMR extends Configured implements Tool
{
  
	public static void main(String args[])
	{
		try {
			SensorMR driver = new SensorMR();
			 int exitCode = ToolRunner.run(driver, args);
			 System.exit(exitCode);
		}catch (Exception e) {
		
			e.printStackTrace();
		}
	}
	
@Override
public int run(String[] args) throws Exception 
{
	Configuration conf = new Configuration();
	String hdfsPath = "hdfs://" + "bonsai-master" + ":" + "9000";
	conf.set("fs.default.name", hdfsPath);		
	conf.set("yarn.resourcemanager.address", "bonsai-master:8032");
	conf.set("mapreduce.jobhistory.address", "bonsai-master:10020");		
	conf.set("mapreduce.framework.name", "yarn");

    Job job;
    try {
		job = Job.getInstance(conf, "word count");			
		job.setJarByClass(SensorMR.class);
	    job.setJobName("Sensor");
	    job.setNumReduceTasks(0);
	    job.setMapperClass(SensorMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setInputFormatClass(SensorInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path("/user/jpvel/finskul/sensor.txt"));
	    FileOutputFormat.setOutputPath(job, new Path("/user/jpvel/finskul/custominput-mr-out"));
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
}