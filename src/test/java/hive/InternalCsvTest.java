package hive;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import finskul.ErrorSummary;

public class InternalCsvTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		fail("Not yet implemented");
	}

	@Test
		public void testCreateDatabase() {
			try {
				InternalManagedCsvTable.createDatabase();;
			} catch (ErrorSummary e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}
		
		@Test
		public void testCreateInternalTable()
		{
			try {
				InternalManagedCsvTable.createTable();
			} catch (ErrorSummary e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}
		
		@Test
		public void testCount()
		{
			try {
				InternalManagedCsvTable.doCount();
			} catch (ErrorSummary e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}
		
		@Test
		public void testCleanup()
		{
			try {
				InternalManagedCsvTable.cleanUp();;
			} catch (ErrorSummary e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}

}
