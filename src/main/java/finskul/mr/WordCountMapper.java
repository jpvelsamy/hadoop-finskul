package finskul.mr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>
{
	//Declrations like this are not good for oop, but we are not doing OOP.
	//Meant for performance wrt to JVM
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) 
      {
    	  
        String incomingWord = itr.nextToken();        
		word.set(incomingWord);
		
        context.write(word, one);
      }
    }
}

//Filter
//Extraction
// Adding more context
// Lookup