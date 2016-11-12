package hive;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.compress.DefaultCodec;

import finskul.ErrorSummary;
import finskul.HdfsClient;

public class ExternalSeqTable {

	public static void writeAsSeq() throws ErrorSummary
	{
		Configuration config = HdfsClient.getConfig();
        FileSystem fs;
        BufferedReader reader=null;
        FileReader fReader =null;
		
    	try {
			fs = FileSystem.get(config);			
			Writer writer = SequenceFile.createWriter(config,
					Writer.file(new Path("/user/jpvel/sequence/whitegoods/lg.seq")), 
					Writer.compression(CompressionType.BLOCK, new DefaultCodec()),
					Writer.blockSize(1048576),
					Writer.keyClass(IntWritable.class),
					Writer.valueClass(Text.class)
					);
			
			fReader = new FileReader(Thread.currentThread().getContextClassLoader().getResource("lg.csv").getPath());
			reader = new BufferedReader(fReader);
			
			String line =null;
			int counter=1;
			while((line=reader.readLine()) != null)
			{
				if(counter>1)
					writer.append(new IntWritable(counter), new Text(line));
				counter++;
			}
			writer.close();
		} catch (IOException e) {
			throw new ErrorSummary(e);
		}
    	finally{
			if(fReader!=null)
				try {
					fReader.close();
				} catch (IOException e) {
					throw new ErrorSummary(e);
				}
			if(fReader!=null)
				try {
					fReader.close();
				} catch (IOException e) {
					throw new ErrorSummary(e);
				}
		}
      
    	
	}

	public static void createExternalTable() throws ErrorSummary
		{
			writeAsSeq();
			String createExtTable = "CREATE EXTERNAL TABLE FINSKUL.SEQ_WHITEGOODS"+
														"(PRODUCT STRING,"+
														"MODEL STRING,"+
														"CATEGORY STRING,"+
														"DP DOUBLE,"+
														"MRP DOUBLE,"+
														"WHP DOUBLE) "+
														"ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'"+
														"STORED AS sequencefile "+
														"LOCATION '/user/jpvel/sequence/whitegoods'";
		
			Util.executeStatement(createExtTable);
		}
	
		public static void doCount() throws ErrorSummary {
			String count="SELECT COUNT(*) FROM FINSKUL.SEQ_WHITEGOODS";
			int rows = Util.executeCount(count);		
			System.out.println("TOTAL ROWS IN SEQ_WHITEGOODS= "+rows);
		}
		
		public static void cleanUp() throws ErrorSummary
		{
			String drop="DROP TABLE FINSKUL.SEQ_WHITEGOODS";
			Util.executeStatement(drop);
			String db="DROP DATBASE FINSKUL";
			Util.executeStatement(db);
			HdfsClient.deleteFolder("/user/jpvel/sequence/whitegoods");
		}
		
}
