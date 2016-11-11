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
			fail(ErrorSummary.stackTrace(e));
		}
	}

}
