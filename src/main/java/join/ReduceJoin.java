package join;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * ./bin/hadoop jar /tmp/com.finskul.hadoop-1.0-SNAPSHOT.jar join.ReduceJoin
 * 
 * Test shell script
 * grep /home/jpvel/workspace/edureka/com.finskul.hadoop/src/main/resources/dev/txns -e "4000001" | cut -d',' -f4>/tmp/floatval.txt
 * @author jpvel
 *
 */
public class ReduceJoin extends Configured implements Tool{ 

	public static void main(String[] args) throws Exception 
	{
		try {
			ReduceJoin driver = new ReduceJoin();
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
			job = Job.getInstance(conf, "ReduceSideJoin");
			job.setJarByClass(ReduceJoin.class);
			job.setReducerClass(ReduceJoinReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			MultipleInputs.addInputPath(job, new Path(
					"/user/jpvel/finskul/custs.txt"), TextInputFormat.class,
					CustsMapper.class);
			MultipleInputs.addInputPath(job, new Path(
					"/user/jpvel/finskul/txns.txt"), TextInputFormat.class,
					TxnsMapper.class);
			Path outputPath = new Path("/user/jpvel/finskul/reducejoin-mr-out");
			FileOutputFormat.setOutputPath(job, outputPath);		
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
