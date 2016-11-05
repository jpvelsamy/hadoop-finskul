package dc;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import local.FileUtil;

public class WordCountWorker implements Callable<Map<String, Integer>> {
	
	private Map<String, Integer> wordCount = new HashMap<String, Integer>();
	private String content=null;
	private CountDownLatch latch =null;
	
	
	@Override
	public Map<String, Integer> call() throws Exception 
	{
	
		System.out.println("Word count executing in worker = "+Thread.currentThread().getName());
		String incomingTokens[] = this.content.split(FileUtil.SPLIT_PATTERN);
		for (int i = 0; i < incomingTokens.length; i++) 
		{
		
				String incomingToken = incomingTokens[i];
				if(this.wordCount.containsKey(incomingToken))
				{
					Integer presentValue = this.wordCount.get(incomingToken);
					presentValue = presentValue.intValue()+1;
					this.wordCount.put(incomingToken, presentValue);
				}
				else
				{
					this.wordCount.put(incomingToken, new Integer(1));
				}
		}
		this.latch.countDown();//notify that i am done
		try{}finally{
			System.out.println("Word count completed in worker = "+Thread.currentThread().getName());
		}
		return this.wordCount;
		
	}


	public WordCountWorker(String content,
			CountDownLatch latch) {
		super();
		this.content = content;
		this.latch = latch;
	}


	

}
