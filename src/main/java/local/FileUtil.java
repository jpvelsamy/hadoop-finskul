package local;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import finskul.ErrorSummary;

public class FileUtil {

	public static String readContent(String fileName) throws ErrorSummary
	{
		BufferedReader reader=null;
		FileReader fReader = null;
		try 
		{
			fReader = new FileReader(Thread.currentThread().getContextClassLoader().getResource(fileName).getPath());
			reader = new BufferedReader(fReader);
			
			StringBuilder readContent = new StringBuilder();
			String lines =null;
			while((lines=reader.readLine()) != null)
			{
				readContent.append(lines);
			}
			String contentFromFile = readContent.toString();
			return contentFromFile;
		} catch (FileNotFoundException e) 
		{
			throw new ErrorSummary(e);
		} catch (IOException e) 
		{
			throw new ErrorSummary(e);
		}
		finally{
				if(fReader!=null)
					try {
						fReader.close();
					} catch (IOException e) {
						throw new ErrorSummary(e);
					}
				if(fReader!=null)
					try {
						fReader.close();
					} catch (IOException e) {
						throw new ErrorSummary(e);
					}
		}
	}

	public static final String SPLIT_PATTERN = "(%|-|\\s|/|;|:|#|,|\\{|\\})+";

}
