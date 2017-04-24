package com.manan.dbproject.main;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class TestDavisBase {
	
	static String prompt = "davisql> ";
	static boolean isExit = false;
	static int pageSize = 1024;
	static String version = "v1.0.0";
	static String copyright = "©2016 Manan Lakhani";
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
	/*
	 * RandomAccessFile object to access the davisbase_tables
	 */
	static CreateFile rfTbl;
	
	/*
	 * RandomAccessFile object to access the davisbase_columns
	 */
	static CreateFile rfCol;	
	
	/*
	 * HashMap to store BPlusTree for each table in the database
	 */
	static Map<String, BPlusTree> bPlusTreeMap = new HashMap<String, BPlusTree>();
	
	/*
	 * Method to initialize davisbase_tables and davisbase_columns	
	 */
	public static void initializeMetaData() throws IOException {
		
		File mainDir = new File("Data");

		 if(!mainDir.exists())
		 {
			mainDir.mkdir();
			 
			File subDirectoryOne = new File("Data\\catalog");
			subDirectoryOne.mkdir();
			
			File subDirectoryTwo = new File("Data\\user_data");
			subDirectoryTwo.mkdir();
			
			//Create 
			File fileTbl = new File("Data\\catalog\\davisbase_tables.tbl");			
			rfTbl = new CreateFile(fileTbl, "rw", 512);
			rfTbl.seek(1);
			rfTbl.writeShort(13);
			rfTbl.setLength(512);
			rfTbl.insertIntoDavisBaseTable("davisbase_tables");
			rfTbl.insertIntoDavisBaseTable("davisbase_columns");
			
			
			File fileCol = new File("Data\\catalog\\davisbase_columns.tbl");
			rfCol = new CreateFile(fileCol, "rw", 2048);	
			rfCol.seek(1);
			rfCol.writeShort(13);
			
			rfCol.insertIntoDavisBaseColumn("davisbase_tables", "rowid", "BYTE", "PRI", 1, "NO");
			rfCol.insertIntoDavisBaseColumn("davisbase_tables", "table_name", "TEXT", "", 2, "YES");
			
			rfCol.insertIntoDavisBaseColumn("davisbase_columns", "rowid", "BYTE", "PRI", 1, "NO");
			rfCol.insertIntoDavisBaseColumn("davisbase_columns", "table_name", "TEXT", "", 2, "YES");
			rfCol.insertIntoDavisBaseColumn("davisbase_columns", "column_name", "TEXT", "", 3, "YES");
			rfCol.insertIntoDavisBaseColumn("davisbase_columns", "data_type", "SMALLINT", "", 4, "YES");
			rfCol.insertIntoDavisBaseColumn("davisbase_columns", "column_key", "TEXT", "", 5, "YES");
			rfCol.insertIntoDavisBaseColumn("davisbase_columns", "ordinal_position", "TINYINT", "", 6, "YES");
			rfCol.insertIntoDavisBaseColumn("davisbase_columns", "is_nullable", "TEXT", "", 7, "YES");
			
					
		 }
		 else  //If files already exists
		 {
			File fileTbl = new File("Data\\catalog\\davisbase_tables.tbl");			
			rfTbl = new CreateFile(fileTbl, "rw", 512);
			rfTbl.recordLength = 21;
			rfTbl.numberOfColumns = 2;
			rfTbl.seek(2);
			rfTbl.rfTblPos = rfTbl.readShort();
			rfTbl.numberOfRecords = rfTbl.calculateNumberOfRecords(1);
			rfTbl.recordStartPostion = 8 + rfTbl.numberOfRecords*2;
					
			File fileCol = new File("Data\\catalog\\davisbase_columns.tbl");
			rfCol = new CreateFile(fileCol, "rw", 2048);
			System.out.println("Data folder already exists");
			rfCol.recordLength = 84;
			rfCol.numberOfColumns = 7;
			rfCol.seek(2);
			rfCol.rfColPos = rfCol.readShort();		
			rfCol.numberOfRecords = rfCol.calculateNumberOfRecords(1);
			rfCol.recordStartPostion = 8 + rfCol.numberOfRecords*2;
			
			rfTbl.seek(8);
			int start = rfTbl.readShort() + 1;
			rfTbl.seek(1);
			int count = rfTbl.readByte();
			
			while(count != 0) {
				rfTbl.seek(start);
				String tableName = rfTbl.readLine().substring(0, 20).trim();
				if(!(tableName.equalsIgnoreCase("davisbase_tables")) && !(tableName.equalsIgnoreCase("davisbase_columns"))) {
					File file = new File("Data\\user_data\\" + tableName + ".tbl");
					CreateFile newTable = new CreateFile(file, "rw", pageSize);
					newTable.countPages = (int) ((newTable.length()/512) - 1); //countPages
					newTable.numberOfRecords = newTable.calculateNumberOfRecords(newTable.countPages); //NumberOfRecords
					int values[] = rfCol.calculateNumberOfColumnsAndRecordLength(tableName); //recordLength, numberOfColumns
					newTable.numberOfColumns = values[0];
					newTable.recordLength = values[1];
					
					if(newTable.countPages == 1) {
						newTable.seek(1);
						int numberOfRecords = newTable.readByte();
						
						if(numberOfRecords == 0) {
							newTable.pos = 512;
						} else {
							newTable.seek(2);
							newTable.pos = newTable.readShort(); //pos
						}
						
						newTable.recordStartPostion = 8 + numberOfRecords*2; //recordStartPosition
					}
					else 
					{
						newTable.seek(1024 + (newTable.countPages - 2)*512 + 1);
						int numberOfRecords = newTable.readByte();
						if(numberOfRecords == 0) {
							newTable.pos = 1024 + (newTable.countPages-1)*512;
						} else {
							newTable.seek(1024 + (newTable.countPages - 2)*512 + 2);
							newTable.pos = newTable.readShort(); //pos
						}
						
						newTable.recordStartPostion = (1024 + (newTable.countPages - 2)*512) + 8 + numberOfRecords*2;
					}
					
					BPlusTree bPlusTree = new BPlusTree(newTable);
					bPlusTreeMap.put(tableName, bPlusTree);
				}
				start = start - rfTbl.recordLength;
				count--;
			}
		 }	
	}
	
	//Main method executed on launch of application
	public static void main(String[] args) throws IOException {
		/* Display the welcome screen */
		splashScreen();

		initializeMetaData();
		
		/* Variable to collect user input from the prompt */
		String userCommand = ""; 
		
		while(!isExit) {
			System.out.print(prompt);
			/* toLowerCase() renders command case insensitive */
			userCommand = scanner.next().replace("\n", "").replace("\r", "").trim().toLowerCase();
			parseUserCommand(userCommand);
		}
		System.out.println("Database exited!");

	}
	
	/*
	 *  Display the splash screen
	 */
	public static void splashScreen() {
		System.out.println(line("-",80));
        System.out.println("Welcome to DavisBaseLite"); // Display the string.
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-",80));
	}
	
	/**
	 * @param s The String to be repeated
	 * @param num The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num times.
	 */
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
	/*
	 * This method parses queries
	 */
	public static void parseUserCommand (String userCommand) throws IOException {
		
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		
		switch (commandTokens.get(0)) {
			case "help":
				help();
			break;
			case "version":
				displayVersion();
			break;
			case "create":
				parseCreateString(userCommand);
			break;
			case "insert":
				parseInsertString(userCommand);
			break;
			case "select":
				parseQueryString(userCommand);
			break;
			case "delete":
				parseDeleteString(userCommand);
			break;
			case "show":
				parseShowString(userCommand);
			break;
			case "drop":
				parseDropString(userCommand);
			break;
			case "update":
				parseUpdateString(userCommand);
			break;
			case "quit":
				isExit = true;
			break;
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
			break;
		}
	}
	
	/**
	 *  Help: Display supported commands
	 */
	public static void help() {
		System.out.println(line("*",80));
		System.out.println("SUPPORTED COMMANDS");
		System.out.println("All commands below are case insensitive");
		System.out.println();
		System.out.println("\tVERSION;                                         								Show the program version.");
		System.out.println("\tHELP;                                            								Show this help information");
		System.out.println("\tQUIT;                                            								Exit the program");
		System.out.println("\tCREATE TABLE table_name (column_name1 INT PRI NO,column_name2 data_type2 [NOT NULL],..); Create a new table");
		System.out.println("\tINSERT INTO TABLE [column_list] table_name VALUES (value1,value2,…);   		Insert record into table");
		System.out.println("\tSELECT * FROM table_name;                        								Display all records in the table.");
		System.out.println("\tSELECT * FROM table_name WHERE rowid = <value>;  								Display records whose rowid is <id>.");
		System.out.println("\tDROP TABLE table_name;                          								Remove table data and its schema.");
		System.out.println("\tUPDATE table_name SET column_name = value [WHERE condition];       			Update table data.\"");
		System.out.println("\tDELETE FROM TABLE table_name;        											Delete all records");
		System.out.println("\tDELETE FROM TABLE table_name WHERE row_id = <value>;       					Delete records whose rowid is <id>.");
		System.out.println("\tSHOW tables;           														Display the table names");
		System.out.println();
		System.out.println();
		System.out.println(line("*",80));
	}

	/*
	 * This method displays the version
	 */
	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}


	/** return the DavisBase version */
	public static String getVersion() {
		return version;
	}
	
	public static String getCopyright() {
		return copyright;
	}
	
	/*
	 * This method parses CREATE command
	 */
	private static void parseCreateString(String userCommand) throws IOException {
		/*
		 	CREATE TABLE table_name (
 			column_name1 INT PRI NO,
 			column_name2 data_type2 [NOT NULL],
	 		column_name3 data_type3 [NOT NULL],
	 		...
			);
		 */
		String commandElements[] = userCommand.split("\\(");
		String fetchTableName[] = commandElements[0].split(" ");
		String tableName = fetchTableName[2];
		//System.out.println("Table Name: " + tableName);
		if(rfTbl.checkIfTableAlreadyExists(tableName)) {
			System.out.println("Table already exists!");
			return;
		}
		
		rfTbl.insertIntoDavisBaseTable(tableName);
		rfCol.insertUserColumsEntry(tableName, commandElements[1]);
		
		File f = new File("Data\\user_data\\" + tableName + ".tbl");
		CreateFile newTable = new CreateFile(f, "rw", pageSize);
		newTable.setLength(pageSize);
		
		newTable.seek(4);
		newTable.writeInt(-1);
		
		//Set root pageType
		int rootStart = pageSize - 512;
		newTable.seek(rootStart);
		newTable.writeByte(5);
		
		//Pointing root to first left leaf node
		newTable.seek(rootStart + 4);
		newTable.writeInt(0);
		
		//Set page type
		newTable.setPageType(13);
		
		//Initialize the record length for each new table
		newTable.calculateRecordLength(commandElements[1]);
		//System.out.println(newTable.recordLength);
		
		//Create a BPlusTree
		BPlusTree bPlusTree = new BPlusTree(newTable);

		bPlusTreeMap.put(tableName, bPlusTree);
		
		System.out.println("Create Performed Successfully!");
	}
	
	/*
	 * This method parses Insert command
	 */
	public static void parseInsertString(String userCommand) throws IOException {
		//INSERT INTO TABLE [column_list] table_name VALUES (value1,value2,value3,…);
		String commandElements[] = userCommand.split(" ");
		String tableName = commandElements[4];
		String columnList[] = commandElements[3].substring(1,commandElements[3].length()-1).split(",");
		String columnValues[] = commandElements[6].substring(1,commandElements[6].length()-1).split(",");
		if(((columnList.length == 1) && (columnList[0].equals(""))) || columnList.length == columnValues.length) 
		{
			BPlusTree tree = bPlusTreeMap.get(tableName);
			tree.root.processInsertQuery(tableName, columnList, columnValues, rfTbl, rfCol, tree);
		} 
		else 
		{
			System.out.println("Incorrect format");
		}
		
	}
	
	/*
	 * This method parses Select Command
	 */
	private static void parseQueryString(String userCommand) throws IOException {
		/* SELECT *
		FROM table_name
		WHERE column_name operator value;
		*/
		ArrayList<String> queryStringTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		String tableName = queryStringTokens.get(3);
		String wildCard = queryStringTokens.get(1);
		String deciding_col = null;
		String operator = null;
		String comp_val = null;
		if(queryStringTokens.size()>4){
            deciding_col = queryStringTokens.get(5);
            operator = queryStringTokens.get(6);
            comp_val = queryStringTokens.get(7);
         }
		
		BPlusTree tree = bPlusTreeMap.get(tableName);
		tree.root.processSelectQuery(tableName, wildCard, deciding_col, operator, comp_val, rfTbl, rfCol);		
	}
	
	/*
	 * This method parses Delete command
	 */
	private static void parseDeleteString(String userCommand) throws IOException {
		//DELETE FROM TABLE table_name WHERE row_id = key_value;
		ArrayList<String> queryStringTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		if( (queryStringTokens.size() == 4) || (queryStringTokens.size() == 8) ) {
			String tableName = queryStringTokens.get(3);
			String deciding_col = null;
			String operator = null;
			String comp_val = null;
			if(queryStringTokens.size() > 4){
	            deciding_col = queryStringTokens.get(5);
	            operator = queryStringTokens.get(6);
	            comp_val = queryStringTokens.get(7);
	         }
			BPlusTree tree = bPlusTreeMap.get(tableName);
			tree.root.processDeleteQuery(tableName, deciding_col, operator, comp_val, rfTbl, rfCol);
		}
		else {
			System.out.println("Invalid query format!");
		}
		
	}
	
	/*
	 *  This method parses Show command
	 */
	private static void parseShowString(String userCommand) throws IOException {
		rfTbl.processShowTableQuery();
	}
	
	/*
	 * This method parses Drop command
	 */
	private static void parseDropString(String userCommand) throws IOException {
		//DROP TABLE table_name;
		ArrayList<String> queryStringTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		if(queryStringTokens.size() == 3) {
			String tableName = queryStringTokens.get(2);
			BPlusTree tree = bPlusTreeMap.get(tableName);
			tree.root.processDropString(tableName, rfTbl, rfCol);
			tree.root.close();
			tree.root.file.delete();
		}
		else {
			System.out.println("Invalid query format!");
		}
		
	}
	
	/*
	 * This method parses Update command
	 */
	private static void parseUpdateString(String userCommand) throws IOException {
		// UPDATE table_name SET column_name = value [WHERE condition]
		ArrayList<String> queryStringTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		if( (queryStringTokens.size() == 6) || (queryStringTokens.size() == 10) ) {
			String tableName = queryStringTokens.get(1);
			String columnToBeUpdated = queryStringTokens.get(3);	
			String valueToBeSet = queryStringTokens.get(5);
			String deciding_col = null;
			String operator = null;
			String comp_val = null;
			if(queryStringTokens.size() > 6){
				deciding_col = queryStringTokens.get(7);
				operator = queryStringTokens.get(8);
				comp_val = queryStringTokens.get(9);
				
	         }
			BPlusTree tree = bPlusTreeMap.get(tableName);
			tree.root.processUpdateString(tableName, columnToBeUpdated, valueToBeSet, deciding_col, operator, comp_val, rfTbl, rfCol);	
		}
		else {
			System.out.println("Invalid Query format!");
		}
		
	}	

}
