package hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import finskul.ErrorSummary;
import finskul.HdfsClient;

public class HiveUtil {

	
	public static Connection getConnection() throws ErrorSummary
	{
		Connection conn = null;
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			conn= DriverManager.getConnection("jdbc:hive2://172.17.0.2:10000","jpvel","jpvel");			
			
		} catch (ClassNotFoundException e) {
			throw new ErrorSummary(e);
		} catch (SQLException e) {
			throw new ErrorSummary(e);
		}
		return conn;
	}
	
	
	public static void createDatabase() throws ErrorSummary
	{
		String createDatabase = "CREATE DATABASE FINSKUL";
		executeStatement(createDatabase);
	}

	public static void createExternalTable() throws ErrorSummary
	{
		HdfsClient.writeWhiteGoodsCsv();
		String createExtTable = "CREATE EXTERNAL TABLE WHITEGOODS"+
													"(PRODUCT STRING,"+
													"MODEL STRING,"+
													"CATEGORY STRING,"+
													"DP DOUBLE,"+
													"MRP DOUBLE,"+
													"WHP DOUBLE) "+
													"ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"+
													"LOCATION '/user/jpvel/whitegoods/'";
	
		executeStatement(createExtTable);
	}

	public static void doCount() throws ErrorSummary {
		String count="SELECT COUNT(*) FROM WHITEGOODS";
		int rows = executeCount(count);		
		System.out.println("TOTAL ROWS IN WHITEGOODS= "+rows);
	}
	
	public static void cleanUp() throws ErrorSummary
	{
		String drop="DROP TABLE WHITEGOODS";
		executeStatement(drop);
		HdfsClient.deleteFolder("/user/jpvel/whitegoods");
	}
	
	private static int executeCount(String query) throws ErrorSummary
	{
		Connection conn = getConnection();
		Statement stmt = null;
		int count = 0;
		try
		{
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next())
			{
				count=rs.getInt(1);
			}
		} catch (SQLException e) {
			throw new ErrorSummary(e);
		}
		finally{
			if(stmt!=null)
				try {
					stmt.close();
				} catch (SQLException e1) {
					throw new ErrorSummary(e1);
				}
			if(conn!=null)
				try {
					conn.close();
				} catch (SQLException e) {
					throw new ErrorSummary(e);
				}
		}
		return count;
	}

	private static void executeStatement(String createDatabase)
			throws ErrorSummary {
		Connection conn = getConnection();
		Statement stmt = null;
		try
		{
			stmt = conn.createStatement();
			stmt.executeUpdate(createDatabase);
			
		} catch (SQLException e) {
			throw new ErrorSummary(e);
		}
		finally{
			if(stmt!=null)
				try {
					stmt.close();
				} catch (SQLException e1) {
					throw new ErrorSummary(e1);
				}
			if(conn!=null)
				try {
					conn.close();
				} catch (SQLException e) {
					throw new ErrorSummary(e);
				}
		}
	}
	
	
	
}
