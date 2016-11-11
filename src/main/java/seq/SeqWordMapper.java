package seq;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SeqWordMapper extends Mapper<Text, Text, Text, IntWritable>
{
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
	
	public void map(Text fileName, Text fileContent, Context context) throws IOException, InterruptedException
	{
		StringTokenizer itr = new StringTokenizer(fileContent.toString());
	     while (itr.hasMoreTokens()) 
	      {
	    	  
	        String incomingWord = itr.nextToken();        
	        if(incomingWord.toLowerCase().contains("television") || incomingWord.toLowerCase().startsWith("led") || incomingWord.toLowerCase().contains("smart"))
	        {
	        	word.set(incomingWord);		
	        	context.write(word, one);
	        	context.getCounter("files", fileName.toString()).increment(1);
	        }
	      }
		
	}
}
