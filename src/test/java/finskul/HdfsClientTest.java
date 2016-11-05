package finskul;

import junit.framework.TestCase;

public class HdfsClientTest extends TestCase {

	public void testListFiles() throws ErrorSummary {
		HdfsClient.listFiles();
	}

	public void testWriteWordCountFile() throws ErrorSummary {
		HdfsClient.writeWordCountFile();
	}
	
	public void testSensorFile() throws ErrorSummary {
		HdfsClient.writeSensorFile();
	}
	
	public void testJoin()throws ErrorSummary{
		HdfsClient.writeReduceJoinFiles();
	}
	
	public void testDCMapJoin()throws ErrorSummary{
		{
			HdfsClient.writeMapJoinFiles();
		}
	}
	
}
