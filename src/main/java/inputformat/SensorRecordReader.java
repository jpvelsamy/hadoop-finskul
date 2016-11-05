package inputformat;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;


public class SensorRecordReader extends RecordReader<SensorKey,SensorReading> {

	private SensorKey key;
	private SensorReading value;
	private LineRecordReader reader = new LineRecordReader();
	@Override
	public void close() throws IOException {
		reader.close();
	}

	@Override
	public SensorKey getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public SensorReading getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return reader.getProgress();
	}

	@Override
	public void initialize(InputSplit is, TaskAttemptContext tac)
			throws IOException, InterruptedException {
		reader.initialize(is, tac);
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		boolean gotNextKeyValue = reader.nextKeyValue();
		if(gotNextKeyValue){
			if(key==null){
				key = new SensorKey();
			}
			if(value == null){
				value = new SensorReading();
			}
			Text line = reader.getCurrentValue();
			String[] tokens = line.toString().split("\t");
			key.setSensorType(new Text(tokens[0]));
			key.setTimestamp(new Text(tokens[1]));
			key.setStatus(new Text(tokens[2]));
			value.setValue1(new Text(tokens[3]));
			value.setValue2(new Text(tokens[4]));
		}
		else {
			key = null;
			value = null;
		}
		return gotNextKeyValue;
	}

}












