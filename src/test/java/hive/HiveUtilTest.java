package hive;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import finskul.ErrorSummary;

public class HiveUtilTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testConnection() {
		try {
			HiveUtil.getConnection();
		} catch (ErrorSummary e) {
			fail(ErrorSummary.stackTrace(e));
		}
	}
	
	@Test
	public void testCreateDatabase() {
		try {
			HiveUtil.createDatabase();;
		} catch (ErrorSummary e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCreatExternalTable()
	{
		try {
			HiveUtil.createExternalTable();
		} catch (ErrorSummary e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCount()
	{
		try {
			HiveUtil.doCount();
		} catch (ErrorSummary e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCleanup()
	{
		try {
			HiveUtil.cleanUp();;
		} catch (ErrorSummary e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
}
