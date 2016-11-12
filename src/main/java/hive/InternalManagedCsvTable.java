package hive;

import finskul.ErrorSummary;
import finskul.HdfsClient;

public class InternalManagedCsvTable {

	public static void createTable() throws ErrorSummary
	{
		String destPath = "/user/jpvel/managed/whitegoods/lg.csv";
		HdfsClient.writeWhiteGoodsCsv(destPath);
		String createTable = "CREATE TABLE FINSKUL.MG_WHITEGOODS"+
													"(PRODUCT STRING,"+
													"MODEL STRING,"+
													"CATEGORY STRING,"+
													"DP DOUBLE,"+
													"MRP DOUBLE,"+
													"WHP DOUBLE) "+
													"ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"+
													"LOCATION '/user/jpvel/managed/whitegoods'";
	
		Util.executeStatement(createTable);
	}

	public static void doCount() throws ErrorSummary {
		String count="SELECT COUNT(*) FROM FINSKUL.MG_WHITEGOODS";
		int rows = Util.executeCount(count);		
		System.out.println("TOTAL ROWS IN MG_WHITEGOODS= "+rows);
	}
	
	public static void cleanUp() throws ErrorSummary
	{
		String drop="DROP TABLE FINSKUL.MG_WHITEGOODS";
		Util.executeStatement(drop);
		
		drop="DROP TABLE FINSKUL.WHITEGOODS_load";
		Util.executeStatement(drop);
		
		drop="DROP TABLE FINSKUL.MG_WHITEGOODS_TOTAL";
		Util.executeStatement(drop);
		
		HdfsClient.deleteFolder("/user/jpvel/managed/whitegoods");
	}
	
	public static void createTotalTable() throws ErrorSummary
	{
		String destPath = "/user/jpvel/managed/whitegoods/totaldata.csv";
		HdfsClient.writeTotalGoodsCsv(destPath);
		String createTable = "CREATE TABLE FINSKUL.MG_WHITEGOODS_TOTAL"+
													"(PRODUCT STRING,"+
													"MODEL STRING,"+
													"CATEGORY STRING,"+
													"DP DOUBLE,"+
													"MRP DOUBLE,"+
													"WHP DOUBLE,"
													+ "MFG STRING) "+
													"ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"+
													"LOCATION '/user/jpvel/managed/whitegoods'";
	
		Util.executeStatement(createTable);
	}

}
