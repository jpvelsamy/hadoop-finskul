package hive;

import finskul.ErrorSummary;
import finskul.HdfsClient;

public class InternalManagedCsvTable {

	public static void createDatabase() throws ErrorSummary
	{
		String createDatabase = "CREATE DATABASE FINSKUL";
		Util.executeStatement(createDatabase);
	}
	
	public static void createTable() throws ErrorSummary
	{
		String destPath = "/user/jpvel/managed/whitegoods/lg.csv";
		HdfsClient.writeWhiteGoodsCsv(destPath);
		String createExtTable = "CREATE EXTERNAL TABLE FINSKUL.MG_WHITEGOODS"+
													"(PRODUCT STRING,"+
													"MODEL STRING,"+
													"CATEGORY STRING,"+
													"DP DOUBLE,"+
													"MRP DOUBLE,"+
													"WHP DOUBLE) "+
													"ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"+
													"LOCATION '/user/jpvel/managed/whitegoods'";
	
		Util.executeStatement(createExtTable);
	}

	public static void doCount() throws ErrorSummary {
		String count="SELECT COUNT(*) FROM FINSKUL.MG_WHITEGOODS";
		int rows = Util.executeCount(count);		
		System.out.println("TOTAL ROWS IN MG_WHITEGOODS= "+rows);
	}
	
	public static void cleanUp() throws ErrorSummary
	{
		String drop="DROP TABLE MG_WHITEGOODS";
		Util.executeStatement(drop);
		String db="DROP DATBASE FINSKUL";
		Util.executeStatement(db);
		HdfsClient.deleteFolder("/user/jpvel/managed/whitegoods");
	}

}
