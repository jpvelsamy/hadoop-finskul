package dc;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import finskul.ErrorSummary;
import junit.framework.TestCase;

public class WordCountDCTest extends TestCase {

	public void testCompute() throws InterruptedException, ExecutionException, ErrorSummary
	{
		WordCountMaster master = new WordCountMaster();
		master.doCount();
		Iterator<String> iter = master.getFinalMap().keySet().iterator();
		for(;iter.hasNext();)
		{
			String key = iter.next();
			Integer value = master.getFinalMap().get(key);
			if(value>10 && value <30)
				System.out.println("["+key+"="+value+"]");
		}
	}
}
