package finskul.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import finskul.WordCountMRApp;

public class WordCountPartitioner extends Partitioner <Text, IntWritable>
{

	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) 
	{
		return WordCountMRApp.evenOrOdd(key);
	}

	

}
