package dc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import local.FileUtil;
import finskul.ErrorSummary;

public class WordCountMaster {

	private CountDownLatch latch = new CountDownLatch(3);
	private ExecutorService threadPool = Executors.newFixedThreadPool(3);
	private List<Future<Map<String, Integer>>> futureContainer = new ArrayList<Future<Map<String, Integer>>>();
	private Map<String, Integer> finalMap = new HashMap<String, Integer>(); 
	
	public void doCount() throws ErrorSummary, InterruptedException, ExecutionException
	{
		String fileContent = FileUtil.readContent("curation.final.amazon.txt");
		int partLength = fileContent.length()/3;
		System.out.println("Part length="+partLength);
		
		String firstPart = fileContent.substring(0, partLength);
		String secondPart = fileContent.substring(partLength+1, partLength+partLength);
		String thirdPart = fileContent.substring(partLength+partLength, fileContent.length());
		
		Callable<Map<String, Integer>> wc1 = new WordCountWorker(firstPart, latch);
		Callable<Map<String, Integer>> wc2 = new WordCountWorker(secondPart, latch);
		Callable<Map<String, Integer>> wc3= new WordCountWorker(thirdPart, latch);
		
        Future<Map<String, Integer>> firstResponse = threadPool.submit(wc1);
        futureContainer.add(firstResponse);
        
        Future<Map<String, Integer>> secondResponse = threadPool.submit(wc2);
        futureContainer.add(secondResponse);
        
        Future<Map<String, Integer>> thirdResponse = threadPool.submit(wc3);
        futureContainer.add(thirdResponse);
		
		try
		{
			this.latch.await();
		}
		catch(Exception e)
		{
			throw new ErrorSummary(e);
		}
		
		
		for (Future<Map<String, Integer>> future : futureContainer) {
			Map<String, Integer> response = future.get();
			Set<String> keys = response.keySet();
			for (String incomingToken : keys) 
			{
				if(finalMap.containsKey(incomingToken))
				{
					Integer presentValue = finalMap.get(incomingToken);
					presentValue = presentValue.intValue()+response.get(incomingToken);
					finalMap.put(incomingToken, presentValue);
				}
				else
				{
					finalMap.put(incomingToken, response.get(incomingToken));
				}
			}
		}
		
	}

	public Map<String, Integer> getFinalMap() {
		return finalMap;
	}

	public void setFinalMap(Map<String, Integer> finalMap) {
		this.finalMap = finalMap;
	}
}
