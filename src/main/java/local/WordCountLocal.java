package local;

import java.util.HashMap;
import java.util.Map;

import finskul.ErrorSummary;

public class WordCountLocal 
{

	private Map<String, Integer> wordCount = new HashMap<String, Integer>();
	private String contentFromFile=null;
	
	public void aggregateCount()
	{
		String incomingTokens[] = this.contentFromFile.split("(%|-|\\s|/|;|:|#|,|\\{|\\})+");
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
	}
	
	public void loadAndCount(String fileName) throws ErrorSummary
	{
		this.contentFromFile=FileUtil.readContent(fileName);
		this.aggregateCount();
		//System.out.println(this.wordCount);
	}

	public Map<String, Integer> getWordCount() {
		return wordCount;
	}

	public String getContentFromFile() {
		return contentFromFile;
	}
	
	
}
