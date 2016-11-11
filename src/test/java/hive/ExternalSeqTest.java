package hive;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import finskul.ErrorSummary;

public class ExternalSeqTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testHiveSeqFile() 
	{
		try {
			ExternalSeqTable.writeAsSeq();
		} catch (ErrorSummary e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCreateDatabase() {
		try {
			ExternalSeqTable.createDatabase();;
		} catch (ErrorSummary e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCreatExternalTable()
	{
		try {
			ExternalSeqTable.createExternalTable();
		} catch (ErrorSummary e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCount()
	{
		try {
			ExternalSeqTable.doCount();
		} catch (ErrorSummary e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCleanup()
	{
		try {
			ExternalSeqTable.cleanUp();;
		} catch (ErrorSummary e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
