package inputformat;
import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;




public class SensorInputFormat extends FileInputFormat<SensorKey,SensorReading> {
	
	@Override
	public RecordReader<SensorKey, SensorReading> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		return new SensorRecordReader();
	}	
}
