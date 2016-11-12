package hive;

import finskul.ErrorSummary;
import finskul.HdfsClient;

public class ExternalCsvTable {
	
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
		
		drop="DROP TABLE FINSKUL.WHITEGOODS_WITH_PARTN";
		Util.executeStatement(drop);
		
		HdfsClient.deleteFolder("/user/jpvel/whitegoods");
	}

	public static void createExternalTableWithPartition() throws ErrorSummary
	{
		String destPath = "/user/jpvel/whitegoods/LG/lg_part.csv";
		HdfsClient.writeWhiteGoodsCsv(destPath);
		destPath = "/user/jpvel/whitegoods/SAMSUNG/samsung_part.csv";
		HdfsClient.writeWhiteGoodsCsv(destPath);
		
		String createExtTable = "CREATE EXTERNAL TABLE FINSKUL.WHITEGOODS_WITH_PARTN"+
													"(PRODUCT STRING,"+
													"MODEL STRING,"+
													"CATEGORY STRING,"+
													"DP DOUBLE,"+
													"MRP DOUBLE,"+
													"WHP DOUBLE) "+
													"PARTITIONED BY (MFG STRING) "+
													"ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"+
													"LOCATION '/user/jpvel/whitegoods/'";
		
		Util.executeStatement(createExtTable);
		
		String partition1 = "ALTER TABLE FINSKUL.WHITEGOODS_WITH_PARTN ADD PARTITION (MFG='LG')"+ 
				"location '/user/jpvel/whitegoods/LG'";
		Util.executeStatement(partition1);
		
		String partition2 = "ALTER TABLE FINSKUL.WHITEGOODS_WITH_PARTN ADD PARTITION (MFG='SAMSUNG')"+ 
				"location '/user/jpvel/whitegoods/SAMSUNG'";
		Util.executeStatement(partition2);
		
	}
	
	
	
	
		
	
}
