package hive;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import finskul.ErrorSummary;

public class ExternalCsvTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	
	
	
	
	@Test
	public void testCreatExternalTable()
	{
		try {
			ExternalCsvTable.createExternalTable();
		} catch (ErrorSummary e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCount()
	{
		try {
			ExternalCsvTable.doCount();
		} catch (ErrorSummary e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCreateTableWithPartition() 
	{
		try {
			ExternalCsvTable.createExternalTableWithPartition();
		} catch (ErrorSummary e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCleanup()
	{
		try {
			ExternalCsvTable.cleanUp();;
		} catch (ErrorSummary e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	
}
