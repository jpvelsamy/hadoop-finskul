package mrunit;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import finskul.ErrorSummary;
import finskul.mr.WordCountMapper;
import finskul.mr.WordCountReducer;

public class MRUnitTest {
	MapDriver<Object, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		WordCountMapper mapper = new WordCountMapper();
		WordCountReducer reducer = new WordCountReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMap() {
		System.out.println("hello" + mapDriver);
		mapDriver.withInput(new Text("Somestuff"),
				new Text("Hello Hello world"));
		mapDriver.withOutput(new Text("Hello"), new IntWritable(1));
		mapDriver.withOutput(new Text("Hello"), new IntWritable(1));
		mapDriver.withOutput(new Text("world"), new IntWritable(1));
		try {
			mapDriver.runTest();
		} catch (IOException e) {
			fail(ErrorSummary.stackTrace(e));
		}
	}

	@Test
	public void testReducer() {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("Hello"), values);
		reduceDriver.withOutput(new Text("Hello"), new IntWritable(2));
		try {
			reduceDriver.runTest();
		} catch (IOException e) {
			fail(ErrorSummary.stackTrace(e));
		}
	}

	@Test
	public void testMapReduce() {
		mapReduceDriver.withInput(new Text("Somestuff"),
				new Text("Hello Hello world"));
		
		mapReduceDriver.withOutput(new Text("Hello"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("world"), new IntWritable(1));
		try {
			mapReduceDriver.runTest();
		} catch (IOException e) {
			fail(ErrorSummary.stackTrace(e));
		}
	}
}
