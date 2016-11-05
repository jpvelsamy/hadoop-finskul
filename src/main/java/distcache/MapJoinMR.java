package distcache;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * ./bin/hadoop jar /tmp/com.finskul.hadoop-1.0-SNAPSHOT.jar distcache.MapJoinMR /user/jpvel/finskul/dcinput.txt /user/jpvel/finskul/mapjoin-mr-out
 * @author jpvel
 *
 */
public class MapJoinMR {
	
	
	
	
	
  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
    
    Job job = new Job();
    job.setJarByClass(MapJoinMR.class);
    job.setJobName("DCTest");
    job.setNumReduceTasks(0);
    
    try{
    DistributedCache.addCacheFile(new URI("/user/jpvel/finskul/abc.dat"), job.getConfiguration());
    }catch(Exception e){
    	System.out.println(e);
    }
    
    job.setMapperClass(MapJoinMapper.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.waitForCompletion(true);
    
    
  }
}