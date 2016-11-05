package finskul.local;

import java.util.Iterator;

import finskul.ErrorSummary;
import junit.framework.TestCase;
import local.FileUtil;
import local.WordCountLocal;

public class WordCountLocalTest extends TestCase {

	private static final String WORDCOUNTPROBLEM_TXT = "curation.final.amazon.txt";


	public void testReadContent() throws ErrorSummary 
	{
		WordCountLocal local = new WordCountLocal();
		FileUtil.readContent(WORDCOUNTPROBLEM_TXT);
		System.out.println(local.getContentFromFile());
	}

	
	public void testLoadAndCount() throws ErrorSummary 
	{
		WordCountLocal local = new WordCountLocal();
		local.loadAndCount(WORDCOUNTPROBLEM_TXT);
		Iterator<String> iter = local.getWordCount().keySet().iterator();
		for(;iter.hasNext();)
		{
			String key = iter.next();
			Integer value = local.getWordCount().get(key);
			if(value>10 && value <30)
				System.out.println("["+key+"="+value+"]");
		}
			
	}

}
