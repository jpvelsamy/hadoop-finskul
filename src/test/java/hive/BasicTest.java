package hive;

import static org.junit.Assert.*;
import finskul.ErrorSummary;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BasicTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testConnection() {
		try {
			Util.getConnection();
		} catch (ErrorSummary e) {
			e.printStackTrace();
			
			fail(ErrorSummary.stackTrace(e));
		}
	}

	@Test
	public void testCreateDatabase() {
		try {
			Util.createDatabase();;
		} catch (ErrorSummary e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
}
