package finskul.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import finskul.WordCountMRApp;

public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>
{
	private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
    int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();//increment by 1
      }
      result.set(sum);
      context.write(key, result);
      
      int i = WordCountMRApp.evenOrOdd(key);
		if(i==0)
			context.getCounter(WordCountMRApp.WORDLENGTH.EVENCHAR).increment(1);
		else
			context.getCounter(WordCountMRApp.WORDLENGTH.ODDCHAR).increment(1);
    }
}
