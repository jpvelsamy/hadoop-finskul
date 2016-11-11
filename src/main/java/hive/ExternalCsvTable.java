package hive;

import finskul.ErrorSummary;
import finskul.HdfsClient;

public class ExternalCsvTable {
	
	public static void createDatabase() throws ErrorSummary
	{
		String createDatabase = "CREATE DATABASE FINSKUL";
		Util.executeStatement(createDatabase);
	}

	public static void createExternalTable() throws ErrorSummary
	{
		String destPath = "/user/jpvel/whitegoods/lg.csv";
		HdfsClient.writeWhiteGoodsCsv(destPath);
		String createExtTable = "CREATE EXTERNAL TABLE FINSKUL.WHITEGOODS"+
													"(PRODUCT STRING,"+
													"MODEL STRING,"+
													"CATEGORY STRING,"+
													"DP DOUBLE,"+
													"MRP DOUBLE,"+
													"WHP DOUBLE) "+
													"ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"+
													"LOCATION '/user/jpvel/whitegoods/'";
	
		Util.executeStatement(createExtTable);
	}

	public static void doCount() throws ErrorSummary {
		String count="SELECT COUNT(*) FROM FINSKUL.WHITEGOODS";
		int rows = Util.executeCount(count);		
		System.out.println("TOTAL ROWS IN WHITEGOODS= "+rows);
	}
	

	public static void cleanUp() throws ErrorSummary
	{
		String drop="DROP TABLE FINSKUL.WHITEGOODS";
		Util.executeStatement(drop);
		
		String db="DROP DATABASE FINSKUL";
		Util.executeStatement(db);
		HdfsClient.deleteFolder("/user/jpvel/whitegoods");
	}
	
	
	
	
		
	
}
