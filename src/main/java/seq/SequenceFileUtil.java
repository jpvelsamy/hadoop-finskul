package seq;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;

import finskul.ErrorSummary;
import finskul.HdfsClient;

//filename + content is the mode for now
public class SequenceFileUtil {
	
	private static String inputCromaFileName = "curation.final.croma.txt";
	private static String inputFKFileName = "curation.final.fk.txt";
	private static String inputSDFileName = "curation.final.sd.txt";
	private static String inputAmazonFileName = "curation.final.amazon.txt";
	
	public static void writeLocal() throws ErrorSummary
	{
			Configuration config = new Configuration();
	        FileSystem fs;
	        
	        
			try {
				fs = FileSystem.get(config);
				SequenceFile.Writer writer = new SequenceFile.Writer(fs, config, new Path("/tmp/curation.sample.seq"), Text.class, Text.class);
				String contentFromFile = readFileToString(inputCromaFileName);
		        writer.append(new Text(inputCromaFileName), new Text(contentFromFile));
		        writer.close();
			} catch (IOException e) {
				throw new ErrorSummary(e);
			}
        
	}
	
	public static String readFileToString(String inputFileName) throws ErrorSummary
	{
		BufferedReader reader=null;
        FileReader fReader =null;
		try {
		
		fReader = new FileReader(Thread.currentThread().getContextClassLoader().getResource(inputFileName).getPath());
		reader = new BufferedReader(fReader);
		
		StringBuilder readContent = new StringBuilder();
		String lines =null;
		while((lines=reader.readLine()) != null)
		{
			readContent.append(lines);
		}
		 String contentFromFile = readContent.toString();
		 return contentFromFile;
		}
		catch (IOException e) {
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
	public static void writeHDFS() throws ErrorSummary
	{
		//FileSystem fs = HdfsClient.getFS();
		Configuration conf = HdfsClient.getConfig();
		try {
			Writer writer = SequenceFile.createWriter(conf,
					Writer.file(new Path("/user/jpvel/finskul/curation.input.1.seq")), 
					Writer.compression(CompressionType.BLOCK, new DefaultCodec()),
					Writer.blockSize(4096),
					Writer.keyClass(Text.class),
					Writer.valueClass(Text.class)
					);
			String fileContent = readFileToString(inputCromaFileName);
			writer.append(new Text(inputCromaFileName), new Text(fileContent));
			fileContent = readFileToString(inputAmazonFileName);
			writer.append(new Text(inputAmazonFileName), new Text(fileContent));
			fileContent = readFileToString(inputSDFileName);
			writer.append(new Text(inputSDFileName), new Text(fileContent));
			fileContent = readFileToString(inputFKFileName);
			writer.append(new Text(inputFKFileName), new Text(fileContent));
			writer.close();
		} catch (IllegalArgumentException | IOException e) {
			throw new ErrorSummary(e);
		}
		
		/*		, 
				Text.class, Text.class, 
				1024, 3, 4096, false, SequenceFile.CompressionType.BLOCK, new DefaultCodec(), );*/
	}
}
