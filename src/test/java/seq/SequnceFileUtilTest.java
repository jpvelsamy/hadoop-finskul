package seq;

import finskul.ErrorSummary;
import junit.framework.TestCase;

public class SequnceFileUtilTest extends TestCase {

	public void testWriteLocal()
	{
		try {
			SequenceFileUtil.writeLocal();
		} catch (ErrorSummary e) {
			fail(ErrorSummary.stackTrace(e));
		}
	}
	
	public void testWriteHDFS()
	{
		try {
			SequenceFileUtil.writeHDFS();;
		} catch (ErrorSummary e) {
			fail(ErrorSummary.stackTrace(e));
		}
	}
}
