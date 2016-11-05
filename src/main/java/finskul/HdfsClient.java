package finskul;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Please remember these implementations are done understanding the usecases
 * holistically rather than being good examples of programming best practice.
 * However no such bad practices will be entertained for 3rd party api usage
 * 
 * @author jpvel
 *
 */
public class HdfsClient {

	private static Logger logger = LoggerFactory.getLogger(HdfsClient.class);

	public static void listFiles() throws ErrorSummary {
		FileSystem fileSystem = getFS();

		try {
			Path path = new Path("/user/root/input");
			RemoteIterator<LocatedFileStatus> fileList = fileSystem.listFiles(
					path, true);
			for (; fileList.hasNext();) {
				LocatedFileStatus fileInfo = fileList.next();
				logger.info(fileInfo.getPath().getName());
			}
		} catch (FileNotFoundException e) {
			throw new ErrorSummary(e);
		} catch (IOException e) {
			throw new ErrorSummary(e);
		} finally {
			if (fileSystem != null)
				try {
					fileSystem.close();
				} catch (IOException e) {
					throw new ErrorSummary(e);
				}
		}
	}

	public static void writeWordCountFile() throws ErrorSummary 
	{
		String destPath = "/user/jpvel/finskul/curation.final.croma.txt";
		String fileName = "curation.final.croma.txt";
		writeFile(destPath, fileName);
	}

	public static void writeSensorFile() throws ErrorSummary
	{
		String destPath = "/user/jpvel/finskul/sensor.txt";
		String fileName ="sensor.txt";
		writeFile(destPath, fileName);
	}
	
	public static void writeReduceJoinFiles() throws ErrorSummary
	{
		String destPath = "/user/jpvel/finskul/custs.txt";
		String fileName ="custs";
		writeFile(destPath, fileName);
		destPath = "/user/jpvel/finskul/txns.txt";
		fileName ="dcinput";
		writeFile(destPath, fileName);
	}
	
	public static void writeMapJoinFiles() throws ErrorSummary
	{
		String destPath = "/user/jpvel/finskul/abc.dat";
		String fileName ="abc.dat";
		writeFile(destPath, fileName);
		destPath = "/user/jpvel/finskul/dcinput.txt";
		fileName ="dcinput";
		writeFile(destPath, fileName);
	}
	
	public static void writeFile(String destPath, String fileName) throws ErrorSummary {
		FileSystem fileSystem = getFS();		
		Path path = new Path(destPath);
		FSDataOutputStream out = null;
		InputStream in = null;
		try {
			out = fileSystem.create(path);
			in = new BufferedInputStream(Thread.currentThread()
					.getContextClassLoader().getResourceAsStream(fileName));

			byte[] b = new byte[1024];
			int numBytes = 0;
			while ((numBytes = in.read(b)) > 0) {
				out.write(b, 0, numBytes);
			}

		} catch (IOException e) {
			throw new ErrorSummary(e);
		} finally {
			if (out != null)
				try {
					out.close();
				} catch (IOException e) {
					throw new ErrorSummary(e);
				}
			if (in != null)
				try {
					in.close();
				} catch (IOException e) {
					throw new ErrorSummary(e);
				}
		}
	}
	
	

	public static FileSystem getFS() throws ErrorSummary {
		Configuration conf = getConfig();
		FileSystem fileSystem = null;
		try {
			fileSystem = FileSystem.get(conf);
		} catch (IOException e) {
			throw new ErrorSummary(e);
		}
		return fileSystem;
	}

	public static Configuration getConfig() {
		String hdfsPath = "hdfs://" + "bonsai-master" + ":" + "9000";
		Configuration conf = new Configuration();
		conf.set("fs.default.name", hdfsPath);
		// If you dont configure this then you will be using the defaults
		conf.set("dfs.replication", "3");
		conf.set("dfs.blocksize", "4K");
		return conf;
	}

}
