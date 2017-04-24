package com.manan.dbproject.main;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CreateFile extends RandomAccessFile {

	/*
	 * File object for creating RandomAccessFile object
	 */
	File file;
	/*
	 * PageSize
	 */
	int pageSize;
	/*
	 * PageType
	 */
	int pageType;
	/*
	 * Total number of records in a table
	 */
	int numberOfRecords;
	/*
	 * Total number of records in root
	 */
	int rootNumberOfRecords;
	
	/*
	 * Position pointer for root page
	 */
	int rootPos;

	/*
	 * Position pointer for davisbase_tables page
	 */
	int rfTblPos;
	
	/*
	 * Position pointer for davisbase_columns page
	 */
	int rfColPos;
	
	/*
	 * Position pointer for user table page
	 */
	int pos;
	
	int recordStartPostion = 8;
	
	int rowId;
	
	/*
	 * Record Length for a table
	 */
	int recordLength;
	
	/*
	 * Number of columns in a table
	 */
	int numberOfColumns;
	
	/*
	 * Number of pages in a table
	 */
	int countPages = 1;
	
	//constructor
	public CreateFile(File file, String mode, int pageSize) throws FileNotFoundException {
		super(file, mode);
		this.file = file;
		this.pageSize = pageSize;
		pos = pageSize/2;
		rootPos = pageSize;
		rfTblPos = pageSize;
		rfColPos = pageSize;
	}

	public CreateFile(String file, String mode, int pageSize) throws FileNotFoundException {
		super(file, mode);
		this.pageSize = pageSize;
		pos = pageSize/2;
		rootPos = pageSize;
		rfTblPos = pageSize;
		rfColPos = pageSize;
	}

	/*
	 * This method sets the page type
	 */
	public void setPageType(int pageType) throws IOException {
		this.pageType = pageType;
		seek(0);
		writeByte(pageType);
	}
	
	/*
	 * This method inserts record into davisbase_tables table
	 */
	public void insertIntoDavisBaseTable(String tableName) throws IOException {
		
		recordLength = 1 + 20;
		
		//Set Number of records
		seek(1);
		numberOfRecords += 1;
		writeByte(numberOfRecords);
		
		rfTblPos = rfTblPos - recordLength;
		
		//Set Address of last record inserted
		seek(2);
		writeShort(rfTblPos);
		
		//Set Address for each record
		seek(recordStartPostion);
		writeShort(rfTblPos);
		recordStartPostion += 2;
		
		seek(rfTblPos);
		writeByte(++rowId);
		
		seek(rfTblPos+1);
		writeBytes(tableName);	
		
		numberOfColumns = 2;
	}

	/*
	 * This method inserts initial records into davisbase_columns table
	 */
	public void insertIntoDavisBaseColumn(String tableName, String columnName, String data_type, String columnKey, int ordinal_position, String is_nullable) throws IOException {
		
		//recordLength = rowId + tableName + columnName + serialCode + columnKey + ordinalPosition + IsNullable
		recordLength = 1 + 20 + 20 + 2 + 20 + 1 + 20; //84 bytes, 2 is for serial code
		
		seek(1);
		//System.out.println(numberOfRecords);
		numberOfRecords += 1;
		writeByte(numberOfRecords);
		
		rfColPos = rfColPos - recordLength;
		//System.out.println(rfColPos);
		
		//Set Address of last record inserted
		seek(2);
		writeShort(rfColPos);
		
		seek(recordStartPostion);
		writeShort(rfColPos);
		recordStartPostion += 2;
		
		seek(rfColPos);
		writeByte(++rowId);
		seek(rfColPos+1);
		writeBytes(tableName);
		seek(rfColPos+21);
		writeBytes(columnName);
		seek(rfColPos+41);
		int serialCode = getSerialCode(data_type);
		writeShort(serialCode);
		seek(rfColPos+43);
		writeBytes(columnKey);
		seek(rfColPos+63);
		writeByte(ordinal_position);
		seek(rfColPos+64);
		writeBytes(is_nullable);
		
		numberOfColumns = 7;
	}

	/*
	 * This method inserts user table column names into davisbase_columns table
	 */
	public void insertUserColumsEntry(String tableName, String columnDetails) throws IOException {
		columnDetails = columnDetails.substring(0, columnDetails.length()-1);
		String columns[] = columnDetails.split(",");
		
		for (int i = 0; i < columns.length; i++) {
			//colname, data_type, pri, nullable
			seek(1);
			int numberOfRecords = readByte();
			seek(1);
			writeByte(++numberOfRecords);
			++this.numberOfRecords;
			
			rfColPos = rfColPos - recordLength;
			
			//Set Address for each record
			seek(recordStartPostion);
			writeShort(rfColPos);
			recordStartPostion += 2;
			
			seek(rfColPos);
			//writeByte(++rowId);
			writeByte(numberOfRecords);
			seek(rfColPos+1);
			writeBytes(tableName);
			seek(rfColPos+21);
			
			String temp[] = new String[4];
			
			String temp1[] = columns[i].split(" ");
			
			for (int j = 0; j < temp1.length; j++) {
				temp[j] = temp1[j];
			}
			
			writeBytes(temp[0]); //ColumnName
			seek(rfColPos+41);
			
			int serialCode = getSerialCode(temp[1]);
			writeShort(serialCode); //DataType
			seek(rfColPos+43);
			
			if(temp[2] != null && temp[2].equalsIgnoreCase("PRI"))
				writeBytes(temp[2]); //ColumnKey
			else {
				writeBytes("");
				temp[3] = temp[2];
			}
			seek(rfColPos+63);
			
			writeByte(i+1);						  //OrdinalPosition
			seek(rfColPos+64);
			
			if(temp[3] != null)
				writeBytes(temp[3]); //IsNullable
			else
				writeBytes("");
		}
		
		seek(2);
		writeShort(rfColPos);
	}
	
	/*
	 * This method returns the serial code for a given data type
	 */
	public int getSerialCode(String data_type) {
		int serialCode = 0;
		if(data_type.equalsIgnoreCase("int"))
        {
            serialCode=0x06;
        }
        else if(data_type.equalsIgnoreCase("tinyint"))
        {
            serialCode=0x04;
        }
        else if(data_type.equalsIgnoreCase("smallint"))
        {
            serialCode=0x05;
        }
        else if(data_type.equalsIgnoreCase("bigint"))
        {
            serialCode=0x07;
        }
        else if(data_type.equalsIgnoreCase("real"))
        {
            serialCode=0x08;
        }
        else if(data_type.equalsIgnoreCase("double"))
        {
            serialCode=0x09;
        }
        else if(data_type.equalsIgnoreCase("datetime"))
        {
            serialCode=0x0A;
        }
        else if(data_type.equalsIgnoreCase("date"))
        {
            serialCode=0x0B;
        }
        else if(data_type.equalsIgnoreCase("text"))
        {
            serialCode=0x0C;
        }		
		return serialCode;
	}
	
	/*
	 * This method returns the data type for a given serial code
	 */
	public String getSerialCodeAsString(int SerialCode) {
		String data_type = "";
		if(SerialCode == 6)
        {
			data_type="int";
        }
        else if(SerialCode == 4)
        {
        	data_type="tinyint";
        }
        else if(SerialCode == 5)
        {
        	data_type="smallint";
        }
        else if(SerialCode == 7)
        {
        	data_type="bigint";
        }
        else if(SerialCode == 8)
        {
        	data_type="real";
        }
        else if(SerialCode == 9)
        {
        	data_type= "double";
        }
        else if(SerialCode == 10)
        {
        	data_type="datetime";
        }
        else if(SerialCode == 11)
        {
        	data_type="date";
        }
        else if(SerialCode == 12)
        {
        	data_type="text";
        }
		return data_type;
	}
	
	/*
	 * This method calculates the record length
	 */
	public void calculateRecordLength(String columnDetails) {
		columnDetails = columnDetails.substring(0, columnDetails.length()-1);
		String columns[] = columnDetails.split(",");
		numberOfColumns = columns.length;
		
		for (int i = 0; i < columns.length; i++) {
			String temp1[] = columns[i].split(" ");
			recordLength += getRecordLengthForDataType(temp1[1]);
		}
		System.out.println("Record Length: " + recordLength);
	}

	/*
	 * This method returns the record length for a given data type
	 */
	private int getRecordLengthForDataType(String data_type) {
		int record_size = 0;
		if(data_type.equalsIgnoreCase("int"))
        {
            record_size=record_size+4;
        }
        else if(data_type.equalsIgnoreCase("tinyint"))
        {
            record_size=record_size+1;
        }
        else if(data_type.equalsIgnoreCase("smallint"))
        {
            record_size=record_size+2;
        }
        else if(data_type.equalsIgnoreCase("bigint"))
        {
            record_size=record_size+8;
        }
        else if(data_type.equalsIgnoreCase("real"))
        {
            record_size=record_size+4;
        }
        else if(data_type.equalsIgnoreCase("double"))
        {
            record_size=record_size+8;
        }
        else if(data_type.equalsIgnoreCase("datetime"))
        {
            record_size=record_size+8;
        }
        else if(data_type.equalsIgnoreCase("date"))
        {
            record_size=record_size+8;
        }
        else if(data_type.equalsIgnoreCase("text"))
        {
            record_size=record_size+20;
        }
		return record_size;
	}


	/*
	 * This method executes the INSERT Query
	 */
	public void processInsertQuery(String tableName, String[] columnList, String[] columnValues, CreateFile rfTbl, CreateFile rfCol, BPlusTree tree) throws IOException {
		int numberOfRecords;
		if(countPages == 1) {
			seek(1);
			numberOfRecords = readByte();
		} else {
			seek(pageSize + (countPages-2)*512 + 1);
			numberOfRecords = readByte();
		}
		
		
		
		if(((pageSize/2) - ((numberOfRecords*recordLength)+8+(numberOfRecords*2)))> recordLength) {
			//Do the normal insert because there is no overflow
		} 
		else //Overflow
		{
			//Creating space for right child
			setLength(pageSize + countPages*512);
			
			//First overflow
			if(countPages == 1) 
			{
				//Left child pointing to right child
				seek(4);
				writeInt(pageSize);
				
				//Right child pointing to FFFF
				seek(pageSize + (countPages-1)*512 + 4);
				//writeInt(Integer.MAX_VALUE);
				writeInt(-1);
			} 
			else 
			{
				//Left child pointing to right child
				seek(pageSize + (countPages-2)*512 + 4);
				writeInt(pageSize + (countPages-1)*512);
				
				//Right child pointing to FFFF
				seek(pageSize + (countPages-1)*512 + 4);
				//writeInt(Integer.MAX_VALUE);
				writeInt(-1);
			}	
			
			//Writing the key where split occurs to the root
			seek(pos);
			int rootKey = readInt();
			System.out.println("Root Key for Split: " + rootKey);
			
			int end = rootPos;
			rootPos = end - 4;
			seek(rootPos);
			writeInt(rootKey);
			
			//Increasing the number of records in root
			seek(512+1);
			write(++rootNumberOfRecords);
			
			//Point pos to the end of new page
			pos = pageSize + countPages*512;
			
			//Set Page Type of new leaf page
			//seek(pos-(countPages*512));
			seek(pageSize+(countPages-1)*512);
			writeByte(13);
			
			//Increment number of pages
			countPages++;
			
			//Reset numberOfRecords to zero for the newly created page
			//numberOfRecords = 0;
			
			recordStartPostion = pageSize + (countPages - 2)*512 + 8;
			 
		}
			
		if(countPages == 1) {
			rfCol.seek(2);
			int position = rfCol.readShort() + 1; // this position will give us the table name
			rfCol.seek(position);
			String referenceTableLine = rfCol.readLine().substring(0, 20);
			
			//System.out.println("Our table: " + tableName);
			//System.out.println("Reference Table name: " + referenceTableLine);
			
			int k = 1;
			while(!referenceTableLine.contains(tableName)) { //readLine gives us the table name	
				position = position + 84*(k);
				rfCol.seek(position);
				referenceTableLine = rfCol.readLine().substring(0, 20);
				//System.out.println("Inside Reference Table name: " + referenceTableLine);
			}

			if(!( (columnList.length == 1) && (columnList[0].equals("")) )) {
				boolean flagForPrimaryKey = checkIfPrimaryKeyIsPresent(columnList, rfCol, position);
				if(!flagForPrimaryKey){
					System.out.println("Primary key not present");
					return;
				}
			}
			
			if(!( (columnList.length == 1) && (columnList[0].equals("")) )) {
				int flagForNotNullableKey = checkIfNotNullableKeyIsPresent(columnList, columnValues, rfCol, position);
				if(flagForNotNullableKey == 0) {
					System.out.println("Not Nullable Column missing");
					return;
				} else if(flagForNotNullableKey == 2) {
					System.out.println("Incorrect value for not nullable column");
					return;
				}
			}
			
			boolean testUniqueness = checkIfPrimaryKeyIsUnique(Integer.parseInt(columnValues[0]));
			if(!testUniqueness) {
				System.out.println("Key is not unique");
				return;
			}
			
			seek(1);
			int numbOfRecords = readByte();
			seek(1);
			writeByte(++numbOfRecords);
			++this.numberOfRecords;
			
			for(int i = columnValues.length - 1; i >= 0; i--){
				
				rfCol.seek(position + 40);
				String data_type = getSerialCodeAsString(rfCol.readShort());
				//System.out.println(data_type);
				
				if(data_type.equalsIgnoreCase("int"))
				{
					pos = pos - 4;
					seek(pos);
					if("null".equalsIgnoreCase(columnValues[i]))
						writeInt(0);
					else
						writeInt((Integer.parseInt(columnValues[i])));
				} 
				else if(data_type.equalsIgnoreCase("tinyint"))
		        {
					pos = pos - 1;
					seek(pos);
					if("null".equalsIgnoreCase(columnValues[i]))
						writeByte(0);
					else
						writeByte((Integer.parseInt(columnValues[i])));
					
		        }
		        else if(data_type.equalsIgnoreCase("smallint"))
		        {
		        	pos = pos - 2;
		        	seek(pos);
		        	if("null".equalsIgnoreCase(columnValues[i]))
						writeShort(0);
					else
						writeShort((Short.parseShort(columnValues[i])));
					
		        }
		        else if(data_type.equalsIgnoreCase("bigint"))
		        {
		        	pos = pos - 8;
		        	seek(pos);
		        	if("null".equalsIgnoreCase(columnValues[i]))
						writeLong(0);
					else
						writeLong((Long.parseLong(columnValues[i])));
					
		        }
		        else if(data_type.equalsIgnoreCase("real"))
		        {
		        	pos = pos - 4;
		        	seek(pos);
		        	if("null".equalsIgnoreCase(columnValues[i]))
						writeFloat(0);
					else
						writeFloat((Float.parseFloat(columnValues[i])));
					
		        }
		        else if(data_type.equalsIgnoreCase("double"))
		        {
		        	pos = pos - 8;
		        	seek(pos);
		        	if("null".equalsIgnoreCase(columnValues[i]))
		        		writeDouble(0);
					else
						writeDouble((Double.parseDouble(columnValues[i])));
		        }
		        else if(data_type.equalsIgnoreCase("datetime"))
		        {
		        	pos = pos - 8;
		        	seek(pos);
		        	if("null".equalsIgnoreCase(columnValues[i]))
		        		writeLong(0);
		        	else {
		        		String dateParams[] = columnValues[i].split("-");
		        		ZoneId zoneId = ZoneId.of( "America/Chicago");
		        		
		        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
		        		
		        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
		        		/* ZonedDateTime toLocalDate() method will display in a simple format */
		        		//System.out.println(zdt.toLocalDate()); 
		        		
		        		/* Convert a ZonedDateTime object to epochSeconds
		        		 * This value can be store 8-byte integer to a binary
		        		 * file using RandomAccessFile writeLong()
		        		 */
		        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
		        		writeLong ( epochSeconds );
		        		
		        	}
		        	
		        }
		        else if(data_type.equalsIgnoreCase("date"))
		        {
		        	pos = pos - 8;
		        	seek(pos);
		        	if("null".equalsIgnoreCase(columnValues[i]))
		        		writeLong(0);
		        	else {
		        		String dateParams[] = columnValues[i].split("-");
		        		ZoneId zoneId = ZoneId.of( "America/Chicago");
		        		
		        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
		        		
		        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
		        		/* ZonedDateTime toLocalDate() method will display in a simple format */
		        		//System.out.println(zdt.toLocalDate()); 
		        		
		        		/* Convert a ZonedDateTime object to epochSeconds
		        		 * This value can be store 8-byte integer to a binary
		        		 * file using RandomAccessFile writeLong()
		        		 */
		        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
		        		writeLong ( epochSeconds );
		        		
		        	}
					
		        }
		        else if(data_type.equalsIgnoreCase("text"))
		        {
		        	pos = pos - 20;
		        	seek(pos);
		        	if("null".equalsIgnoreCase(columnValues[i]))
		        		writeBytes("");
		        	else
		        		writeBytes(columnValues[i]);
					
		        }
				position = position + 84;
				
				}
				//Update Byte 2 in new table
				seek(2);
				writeShort(pos);
				
				seek(recordStartPostion);
				writeShort(pos);
				recordStartPostion += 2;
				}
			else //Overflow Insert
			{
				rfCol.seek(2);
				int position = rfCol.readShort() + 1; // this position will give us the table name
				rfCol.seek(position);
				String referenceTableLine = rfCol.readLine().substring(0, 20);
				
				//System.out.println("Our table: " + tableName);
				//System.out.println("Reference Table name: " + referenceTableLine);
				
				//tableName.equalsIgnoreCase(referenceTableLine)
				int k = 1;
				while(!referenceTableLine.contains(tableName)) { //readLine gives us the table name	
					position = position + 84*(k);
					rfCol.seek(position);
					referenceTableLine = rfCol.readLine().substring(0, 20);
					//System.out.println("Inside Reference Table name: " + referenceTableLine);
					//k++;
				}

				if(((columnList.length == 1) && (columnList[0].equals("")))) {
					boolean flagForPrimaryKey = checkIfPrimaryKeyIsPresent(columnList, rfCol, position);
					if(!flagForPrimaryKey){
						System.out.println("Primary key not present");
						return;
					}
				}
				
				if(((columnList.length == 1) && (columnList[0].equals("")))) {
					int flagForNotNullableKey = checkIfNotNullableKeyIsPresent(columnList, columnValues, rfCol, position);
					if(flagForNotNullableKey == 0) {
						System.out.println("Not Nullable Column missing");
						return;
					} else if(flagForNotNullableKey == 2) {
						System.out.println("Incorrect value for not nullable column");
						return;
					}
				}
				
				
				boolean testUniqueness = checkIfPrimaryKeyIsUnique(Integer.parseInt(columnValues[0]));
				if(!testUniqueness) {
					System.out.println("Key is not unique");
					return;
				}
				
				seek(pageSize + (countPages - 2)*512 + 1);
				int n = readByte();
				seek(pageSize + (countPages - 2)*512 + 1);
				writeByte(++n);
				
				for(int i = columnValues.length - 1; i >= 0; i--){
					
					//int position = rfCol.readShort() + (84*(columnValues.length-(i+1)) + 41); 		//columnnumbr*50 + 41 to get serial code of data type
					
					rfCol.seek(position + 40);
					String data_type = getSerialCodeAsString(rfCol.readShort());
					//System.out.println(data_type);
					
					if(data_type.equalsIgnoreCase("int")){
						pos = pos - 4;
						seek(pos);
						if("null".equalsIgnoreCase(columnValues[i]))
							writeInt(0);
						else
							writeInt((Integer.parseInt(columnValues[i])));
						//seek(pos+4);
					} 
					else if(data_type.equalsIgnoreCase("tinyint"))
			        {
						pos = pos - 1;
						seek(pos);
						if("null".equalsIgnoreCase(columnValues[i]))
							writeByte(0);
						else
							writeByte((Integer.parseInt(columnValues[i])));
						
			        }
			        else if(data_type.equalsIgnoreCase("smallint"))
			        {
			        	pos = pos - 2;
			        	seek(pos);
			        	if("null".equalsIgnoreCase(columnValues[i]))
							writeShort(0);
						else
							writeShort((Short.parseShort(columnValues[i])));
						
			        }
			        else if(data_type.equalsIgnoreCase("bigint"))
			        {
			        	pos = pos - 8;
			        	seek(pos);
			        	if("null".equalsIgnoreCase(columnValues[i]))
							writeLong(0);
						else
							writeLong((Long.parseLong(columnValues[i])));
						
			        }
			        else if(data_type.equalsIgnoreCase("real"))
			        {
			        	pos = pos - 4;
			        	seek(pos);
			        	if("null".equalsIgnoreCase(columnValues[i]))
							writeFloat(0);
						else
							writeFloat((Float.parseFloat(columnValues[i])));
						
			        }
			        else if(data_type.equalsIgnoreCase("double"))
			        {
			        	pos = pos - 8;
			        	seek(pos);
			        	if("null".equalsIgnoreCase(columnValues[i]))
			        		writeDouble(0);
						else
							writeDouble((Double.parseDouble(columnValues[i])));
			        }
			        else if(data_type.equalsIgnoreCase("datetime"))
			        {
			        	pos = pos - 8;
			        	seek(pos);
			        	if("null".equalsIgnoreCase(columnValues[i]))
			        		writeLong(0);
			        	else {
			        		String dateParams[] = columnValues[i].split("-");
			        		ZoneId zoneId = ZoneId.of( "America/Chicago");
			        		
			        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
			        		
			        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
			        		/* ZonedDateTime toLocalDate() method will display in a simple format */
			        		//System.out.println(zdt.toLocalDate()); 
			        		
			        		/* Convert a ZonedDateTime object to epochSeconds
			        		 * This value can be store 8-byte integer to a binary
			        		 * file using RandomAccessFile writeLong()
			        		 */
			        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
			        		writeLong ( epochSeconds );
			        		
			        	}
						
			        }
			        else if(data_type.equalsIgnoreCase("date"))
			        {
			        	pos = pos - 8;
			        	seek(pos);
			        	if("null".equalsIgnoreCase(columnValues[i]))
			        		writeLong(0);
			        	else {
			        		String dateParams[] = columnValues[i].split("-");
			        		ZoneId zoneId = ZoneId.of( "America/Chicago");
			        		
			        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
			        		
			        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
			        		/* ZonedDateTime toLocalDate() method will display in a simple format */
			        		//System.out.println(zdt.toLocalDate()); 
			        		
			        		/* Convert a ZonedDateTime object to epochSeconds
			        		 * This value can be store 8-byte integer to a binary
			        		 * file using RandomAccessFile writeLong()
			        		 */
			        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
			        		writeLong ( epochSeconds );
			        		
			        	}
						
			        }
			        else if(data_type.equalsIgnoreCase("text"))
			        {
			        	pos = pos - 20;
			        	seek(pos);
			        	if("null".equalsIgnoreCase(columnValues[i]))
			        		writeBytes("");
			        	else
			        		writeBytes(columnValues[i]);
						
			        }
					position = position + 84;
					
				}
				//Update Byte 2 in new table
				seek(pageSize + (countPages - 2)*512 + 2);
				writeShort(pos);
				
				//Update position of each record
				seek(recordStartPostion);
				writeShort(pos);
				recordStartPostion += 2;
			}
			
		System.out.println("Record Inserted successfully!");
		} 

	/*
	 * This method checks if primary key is unique for the record which is inserted
	 */
	private boolean checkIfPrimaryKeyIsUnique(Integer key) throws IOException {
		int page = 1;
		while(page <= countPages) {
			if(page == 1) {
				seek(1);
				int numberOfRecords = readByte();
				seek(8);
				int checkStart = 0;
				if(numberOfRecords > 0) {
					checkStart = readShort();
				}
				int numberOfIterations = numberOfRecords;
				while(numberOfIterations != 0) {
					seek(checkStart);
					if(readInt() == key){
						return false;
					}
					numberOfIterations--;
					checkStart = checkStart - recordLength;
				}
				
			} 
			else 
			{
				seek(pageSize + (page - 2)*512 + 1);
				int numberOfRecords = readByte();
				seek(pageSize + (page - 2)*512 + 8);
				int checkStart = 0;
				if(numberOfRecords > 0) {
					checkStart = readShort();
				}
				int numberOfIterations = numberOfRecords;
				while(numberOfIterations != 0) {
					seek(checkStart);
					if(readInt() == key){
						return false;
					}
					numberOfIterations--;
					checkStart = checkStart - recordLength;
				}
				
			}
			page++;
		}
		
		return true;		
	}

	/*
	 * This method checks if not nullable key is present in the insert query
	 */
	private int checkIfNotNullableKeyIsPresent(String[] columnList, String[] columnValues, CreateFile rfCol, int position) throws IOException {
		
		for (int i = numberOfColumns-1; i >= 0; i--) {
			//boolean flag = false;
			int status = 0;
			rfCol.seek(position + 63);
			String referenceNotNullableColumnName = null;
			String referenceKey = rfCol.readLine().substring(0, 20);
			//System.out.println("Reference Is Nullable: " + referenceKey);
			
			if(referenceKey != null && referenceKey.contains("no")){
				rfCol.seek(position + 20);
				referenceNotNullableColumnName = rfCol.readLine().substring(0, 20);
				//System.out.println("column details: "+referenceNotNullableColumnName);
				
				
				for (int j = 0; j < columnList.length; j++) {
					if(referenceNotNullableColumnName != null && referenceNotNullableColumnName.contains(columnList[j])) {
						//flag = true;
						status = 1;
						if("null".equalsIgnoreCase(columnValues[j])) {
							System.out.println("Null value is present for not nullable key");
							status = 2;
						}	
							
					}
				}
				if(status == 0 || status == 2){
					return status;
				}
						
			}
			position = position + 84;
		}
		return 1;
	}	

	/*
	 * This method checks if not primary key is present in the insert query
	 */
	private boolean checkIfPrimaryKeyIsPresent(String[] columnList, CreateFile rfCol, int position) throws IOException {
		boolean flag = false;
		for (int i = numberOfColumns-1; i >= 0; i--) {
			rfCol.seek(position + 42);
			String referencePrimaryColumnName = null;
			String referenceKey = rfCol.readLine().substring(0, 20);
			//System.out.println("Reference: " + referenceKey);
			
			if(referenceKey != null && referenceKey.contains("pri")){
				rfCol.seek(position + 20);
				referencePrimaryColumnName = rfCol.readLine().substring(0, 20);
				//System.out.println("column details: "+referencePrimaryColumnName);
				
				for (int j = 0; j < columnList.length; j++) {
					if(referencePrimaryColumnName != null && referencePrimaryColumnName.contains(columnList[j])) {
						return true;
					}
				}
						
			}
			position = position + 84;
		}
		return flag;
	}


	/*
	 * This method executes the SELECT Query
	 */
	public void processSelectQuery(String tableName, String wildCard, String deciding_col, String operator, String comp_val, CreateFile rfTbl, CreateFile rfCol) throws IOException {

		//Find the position where the table name exists in davisbase_columns table
		rfCol.seek(2);
		int position = rfCol.readShort() + 1; // this position will give us the table name
		rfCol.seek(position);
		String referenceTableLine = rfCol.readLine().substring(0, 20);
		
		//System.out.println("Our table: " + tableName);
		//System.out.println("Reference Table name: " + referenceTableLine);
		
		int k = 1;
		while(!referenceTableLine.contains(tableName)) { //readLine gives us the table name	
			position = position + 84*(k);
			rfCol.seek(position);
			referenceTableLine = rfCol.readLine().substring(0, 20);
			//System.out.println("Inside Reference Table name: " + referenceTableLine);
			//k++;
		}
		
		int pageToBeProcessed = 1;
		while(pageToBeProcessed <= countPages) {
			if(pageToBeProcessed == 1) 
			{
				seek(1);
				int numberOfRecords = readByte();
				helperMethodForSelect(pageToBeProcessed, numberOfRecords, wildCard, deciding_col, operator, comp_val, rfTbl, rfCol, position);
			}
			else
			{
				seek(pageSize + (pageToBeProcessed - 2)*512 + 1);
				int numberOfRecords = readByte();
				helperMethodForSelect(pageToBeProcessed, numberOfRecords, wildCard, deciding_col, operator, comp_val, rfTbl, rfCol, position);
			}
			pageToBeProcessed++;
		}
			
	}


	/*
	 * Helper function for SELECT Query
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void helperMethodForSelect(int pageToBeProcessed, int numberOfRecords, String wildCard, String deciding_col, String operator, String comp_val, CreateFile rfTbl, CreateFile rfCol, int position) throws IOException {
		//Get number of records in table
				//seek(1);
				//int numberOfRecords = readByte();
				
				ArrayList[] list = new ArrayList[numberOfColumns];
				String data_type = null;
				int recordStartPosition = 8;
				int numberOfBytes = 0;
				int sum = 0;
				
				
				String columnNames[] = new String[numberOfColumns];
				Map<String, Integer> mapOfOrdinalPostions = new HashMap<String, Integer>();
				Map<String, String> mapOfDataTypes = new HashMap<String, String>();
				
				for (int i = numberOfColumns-1; i >= 0; i--) {
					
					//Get column name
					rfCol.seek(position+20);
					String columnName = rfCol.readLine().substring(0, 20).trim();
					columnNames[i] = columnName;
					
					//Get ordinal position
					rfCol.seek(position+62);
					int ordinalPosition = rfCol.readByte();
					
					//Store it in a map
					mapOfOrdinalPostions.put(columnName, ordinalPosition);
					
					//Get the data type
					rfCol.seek(position + 40);
					data_type = getSerialCodeAsString(rfCol.readShort());
					//System.out.println(data_type);
					
					//Store it in a map
					mapOfDataTypes.put(columnName, data_type);
					
					//Create a empty list based on data type
					list[i] = getArrayList(data_type);
					
					
					int temp = numberOfRecords;	
					numberOfBytes = getRecordLengthForDataType(data_type);
					sum = sum + numberOfBytes;
					
					int start;
					if(pageToBeProcessed == 1) {
						seek(recordStartPosition);
						start = readShort() + recordLength - sum; 	//recordLength - numberOfBytes
						seek(start);
					} else {
						seek(pageSize + (pageToBeProcessed - 2)*512 + recordStartPosition);
						start = readShort() + recordLength - sum; 	//recordLength - numberOfBytes
						seek(start);
					}
					
					while(temp != 0) {
						
						if(data_type.equalsIgnoreCase("int"))
						{
							list[i].add(readInt());
						} 
						else if(data_type.equalsIgnoreCase("tinyint"))
				        {
							list[i].add(readByte());					
				        }
				        else if(data_type.equalsIgnoreCase("smallint"))
				        {
				        	list[i].add(readShort());
				        }
				        else if(data_type.equalsIgnoreCase("bigint"))
				        {
				        	list[i].add(readLong());
				        }
				        else if(data_type.equalsIgnoreCase("real"))
				        {
				        	list[i].add(readFloat());
				        }
				        else if(data_type.equalsIgnoreCase("double"))
				        {
				        	list[i].add(readDouble());
				        }
				        else if(data_type.equalsIgnoreCase("datetime"))
				        {	
				        	list[i].add(readLong());
				        }
				        else if(data_type.equalsIgnoreCase("date"))
				        {
				        	list[i].add(readLong());
						 }
				        else if(data_type.equalsIgnoreCase("text"))
				        {
				        	list[i].add(readLine().substring(0, 20).trim());				        	
				        }
						
						start = start - recordLength;
						seek(start);
						temp--;
					}
					position = position + 84;	
				}
				
				//Print Lists
				ArrayList<Integer> index = new ArrayList<>();
				if(deciding_col != null) {
					int deciding_ordinalPosition = mapOfOrdinalPostions.get(deciding_col);
					String deciding_dataType = mapOfDataTypes.get(deciding_col);
					ArrayList deciding_List = list[deciding_ordinalPosition-1];
					for (int j = 0; j < deciding_List.size(); j++) {
						if("<".equals(operator)) 
						{
							if(deciding_dataType.equalsIgnoreCase("int")) 
							{
								if((Integer)deciding_List.get(j) < Integer.parseInt(comp_val)){
									index.add(j);
								}
							} 
							else if(deciding_dataType.equalsIgnoreCase("tinyint"))
					        {
								if((Byte)deciding_List.get(j) < Byte.parseByte(comp_val)){
									index.add(j);
								}					
					        }
					        else if(deciding_dataType.equalsIgnoreCase("smallint"))
					        {
					        	if((Short)deciding_List.get(j) < Short.parseShort(comp_val)){
									index.add(j);
								}
					        }
					        else if(deciding_dataType.equalsIgnoreCase("bigint"))
					        {
					        	if((Long)deciding_List.get(j) < Long.parseLong(comp_val)){
									index.add(j);
								}
					        }
					        else if(deciding_dataType.equalsIgnoreCase("real"))
					        {
					        	if((Float)deciding_List.get(j) < Float.parseFloat(comp_val)){
									index.add(j);
								}
					        }
					        else if(deciding_dataType.equalsIgnoreCase("double"))
					        {
					        	if((Double)deciding_List.get(j) < Double.parseDouble(comp_val)){
									index.add(j);
								}
					        }
					        else if(deciding_dataType.equalsIgnoreCase("datetime"))
					        {	
					        	String dateParams[] = comp_val.split("-");
					        	ZoneId zoneId = ZoneId.of( "America/Chicago");
				        		
				        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
				        		
				        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
				        		/* ZonedDateTime toLocalDate() method will display in a simple format */
				        		//System.out.println(zdt.toLocalDate()); 
				        		
				        		/* Convert a ZonedDateTime object to epochSeconds
				        		 * This value can be store 8-byte integer to a binary
				        		 * file using RandomAccessFile writeLong()
				        		 */
				        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
					        	
					        	if((Long)deciding_List.get(j) < epochSeconds){
									index.add(j);
								}
					        } 
					        else if(deciding_dataType.equalsIgnoreCase("date")) 
					        {
					        	String dateParams[] = comp_val.split("-");
					        	ZoneId zoneId = ZoneId.of( "America/Chicago");
				        		
				        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
				        		
				        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
				        		/* ZonedDateTime toLocalDate() method will display in a simple format */
				        		//System.out.println(zdt.toLocalDate()); 
				        		
				        		/* Convert a ZonedDateTime object to epochSeconds
				        		 * This value can be store 8-byte integer to a binary
				        		 * file using RandomAccessFile writeLong()
				        		 */
				        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
					        	
					        	if((Long)deciding_List.get(j) < epochSeconds){
									index.add(j);
								}
					        }
			
							
						} 
						else if(">".equals(operator))
						{
							if(deciding_dataType.equalsIgnoreCase("int")) 
							{
								if((Integer)deciding_List.get(j) > Integer.parseInt(comp_val)){
									index.add(j);
								}
							} 
							else if(deciding_dataType.equalsIgnoreCase("tinyint"))
					        {
								if((Byte)deciding_List.get(j) > Byte.parseByte(comp_val)){
									index.add(j);
								}					
					        }
					        else if(deciding_dataType.equalsIgnoreCase("smallint"))
					        {
					        	if((Short)deciding_List.get(j) > Short.parseShort(comp_val)){
									index.add(j);
								}
					        }
					        else if(deciding_dataType.equalsIgnoreCase("bigint"))
					        {
					        	if((Long)deciding_List.get(j) > Long.parseLong(comp_val)){
									index.add(j);
								}
					        }
					        else if(deciding_dataType.equalsIgnoreCase("real"))
					        {
					        	if((Float)deciding_List.get(j) > Float.parseFloat(comp_val)){
									index.add(j);
								}
					        }
					        else if(deciding_dataType.equalsIgnoreCase("double"))
					        {
					        	if((Double)deciding_List.get(j) > Double.parseDouble(comp_val)){
									index.add(j);
								}
					        }
					        else if(deciding_dataType.equalsIgnoreCase("datetime"))
					        {	
					        	String dateParams[] = comp_val.split("-");
					        	ZoneId zoneId = ZoneId.of( "America/Chicago");
				        		
				        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
				        		
				        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
				        		/* ZonedDateTime toLocalDate() method will display in a simple format */
				        		//System.out.println(zdt.toLocalDate()); 
				        		
				        		/* Convert a ZonedDateTime object to epochSeconds
				        		 * This value can be store 8-byte integer to a binary
				        		 * file using RandomAccessFile writeLong()
				        		 */
				        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
					        	
					        	if((Long)deciding_List.get(j) > epochSeconds){
									index.add(j);
								}
					        } 
					        else if(deciding_dataType.equalsIgnoreCase("date")) 
					        {
					        	String dateParams[] = comp_val.split("-");
					        	ZoneId zoneId = ZoneId.of( "America/Chicago");
				        		
				        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
				        		
				        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
				        		/* ZonedDateTime toLocalDate() method will display in a simple format */
				        		//System.out.println(zdt.toLocalDate()); 
				        		
				        		/* Convert a ZonedDateTime object to epochSeconds
				        		 * This value can be store 8-byte integer to a binary
				        		 * file using RandomAccessFile writeLong()
				        		 */
				        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
					        	
					        	if((Long)deciding_List.get(j) > epochSeconds){
									index.add(j);
								}
					        }
						}
						else 
						{ // = operator
							if(deciding_dataType.equalsIgnoreCase("int")) 
							{
								if((Integer)deciding_List.get(j) == Integer.parseInt(comp_val)){
									index.add(j);
								}
							} 
							else if(deciding_dataType.equalsIgnoreCase("tinyint"))
					        {
								if((Byte)deciding_List.get(j) == Byte.parseByte(comp_val)){
									index.add(j);
								}					
					        }
					        else if(deciding_dataType.equalsIgnoreCase("smallint"))
					        {
					        	if((Short)deciding_List.get(j) == Short.parseShort(comp_val)){
									index.add(j);
								}
					        }
					        else if(deciding_dataType.equalsIgnoreCase("bigint"))
					        {
					        	if((Long)deciding_List.get(j) == Long.parseLong(comp_val)){
									index.add(j);
								}
					        }
					        else if(deciding_dataType.equalsIgnoreCase("real"))
					        {
					        	if((Float)deciding_List.get(j) == Float.parseFloat(comp_val)){
									index.add(j);
								}
					        }
					        else if(deciding_dataType.equalsIgnoreCase("double"))
					        {
					        	if((Double)deciding_List.get(j) == Double.parseDouble(comp_val)){
									index.add(j);
								}
					        }
					        else if(deciding_dataType.equalsIgnoreCase("datetime"))
					        {	
					        	String dateParams[] = comp_val.split("-");
					        	ZoneId zoneId = ZoneId.of( "America/Chicago");
				        		
				        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
				        		
				        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
				        		/* ZonedDateTime toLocalDate() method will display in a simple format */
				        		//System.out.println(zdt.toLocalDate()); 
				        		
				        		/* Convert a ZonedDateTime object to epochSeconds
				        		 * This value can be store 8-byte integer to a binary
				        		 * file using RandomAccessFile writeLong()
				        		 */
				        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
					        	
					        	if((Long)deciding_List.get(j) == epochSeconds){
									index.add(j);
								}
					        } 
					        else if(deciding_dataType.equalsIgnoreCase("date")) 
					        {
					        	String dateParams[] = comp_val.split("-");
					        	ZoneId zoneId = ZoneId.of( "America/Chicago");
				        		
				        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
				        		
				        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
				        		/* ZonedDateTime toLocalDate() method will display in a simple format */
				        		//System.out.println(zdt.toLocalDate()); 
				        		
				        		/* Convert a ZonedDateTime object to epochSeconds
				        		 * This value can be store 8-byte integer to a binary
				        		 * file using RandomAccessFile writeLong()
				        		 */
				        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
					        	
					        	if((Long)deciding_List.get(j) == epochSeconds){
									index.add(j);
								}
					        }
					        else if(deciding_dataType.equalsIgnoreCase("text")) 
					        {
								if(((String)deciding_List.get(j)).equals(comp_val)){
									index.add(j);
								}
							}
						}
					}
					
				} 
				
				if(!("*".equals(wildCard))) {
					
					String selectColumnNames[] = wildCard.split(",");
					if(pageToBeProcessed == 1) {
						for (int j = 0; j < selectColumnNames.length; j++) {
							System.out.print(selectColumnNames[j] + " ");
						}
						System.out.println();
					}
					
					
					if(deciding_col != null) {
						
						int m = 0;
						while(m < index.size()){
							for (int z = 0; z < selectColumnNames.length; z++) {
								int temp = mapOfOrdinalPostions.get(selectColumnNames[z]) - 1;
								if(mapOfDataTypes.get(selectColumnNames[z]).equalsIgnoreCase("datetime") || mapOfDataTypes.get(selectColumnNames[z]).equalsIgnoreCase("date"))
								{
									
									/* Define the time zone for Dallas CST */
						        	ZoneId zoneId = ZoneId.of ( "America/Chicago" );
						        	
						        	/* Converst Epoch Seconds back to a new ZonedDateTime object 
						        	 * First use RandomAccessFile readLong() to retrieve 8-byte
						        	 * integer from table file, then...
						        	 */
						        	long retreivedEpochSeconds = (long) list[temp].get(index.get(m));
						        	Instant a = Instant.ofEpochSecond (retreivedEpochSeconds); 
						        	ZonedDateTime zdt2 = ZonedDateTime.ofInstant ( a, zoneId );
						        	
						        	//list[i].add(zdt2.toLocalDate());
									System.out.print(zdt2.toLocalDate() + " ");
								}
								else
								{
									System.out.print(list[temp].get(index.get(m)) + " ");
								}
									
							}
							System.out.println();
							m++;
						}
					} else 
					  {
						
						int m = 0;
						while(m < numberOfRecords){
							for (int z = 0; z < selectColumnNames.length; z++) {
								int temp = mapOfOrdinalPostions.get(selectColumnNames[z]) - 1;
								if(mapOfDataTypes.get(selectColumnNames[z]).equalsIgnoreCase("datetime") || mapOfDataTypes.get(selectColumnNames[z]).equalsIgnoreCase("date"))
								{
									
									/* Define the time zone for Dallas CST */
						        	ZoneId zoneId = ZoneId.of ( "America/Chicago" );
						        	
						        	/* Convert Epoch Seconds back to a new ZonedDateTime object 
						        	 * First use RandomAccessFile readLong() to retrieve 8-byte
						        	 * integer from table file, then...
						        	 */
						        	long retreivedEpochSeconds = (long) list[temp].get(m);
						        	Instant a = Instant.ofEpochSecond (retreivedEpochSeconds); 
						        	ZonedDateTime zdt2 = ZonedDateTime.ofInstant ( a, zoneId );
						        	
						        	//list[i].add(zdt2.toLocalDate());
									System.out.print(zdt2.toLocalDate() + " ");
								}
								else
								{
									System.out.print(list[temp].get(m) + " ");
								}
							
							}
							System.out.println();
							m++;
						}
					}
				} 
				else 
				{	
					if(pageToBeProcessed == 1) {
						for (int j = 0; j < columnNames.length; j++) {
							System.out.print(columnNames[j] + " ");
						}
						System.out.println();
					}
					
					
					
					if (deciding_col != null) {

						int m = 0;
						while (m < index.size()) {
							for (int z = 0; z < columnNames.length; z++) {
								
								if(mapOfDataTypes.get(columnNames[z]).equalsIgnoreCase("datetime") || mapOfDataTypes.get(columnNames[z]).equalsIgnoreCase("date"))
								{
									
									/* Define the time zone for Dallas CST */
						        	ZoneId zoneId = ZoneId.of ( "America/Chicago" );
						        	
						        	/* Convert Epoch Seconds back to a new ZonedDateTime object 
						        	 * First use RandomAccessFile readLong() to retrieve 8-byte
						        	 * integer from table file, then...
						        	 */
						        	long retreivedEpochSeconds = (long)list[z].get(index.get(m));
						        	Instant a = Instant.ofEpochSecond (retreivedEpochSeconds); 
						        	ZonedDateTime zdt2 = ZonedDateTime.ofInstant ( a, zoneId );
						        	
						        	//list[i].add(zdt2.toLocalDate());
									System.out.print(zdt2.toLocalDate() + " ");
								}
								else
								{
									System.out.print(list[z].get(index.get(m)) + " ");
								}
								
							}
							System.out.println();
							m++;
						}
					} 
					else 
					{
						int m = 0;
						while (m < numberOfRecords) {
							for (int z = 0; z < list.length; z++) {
								if(mapOfDataTypes.get(columnNames[z]).equalsIgnoreCase("datetime") || mapOfDataTypes.get(columnNames[z]).equalsIgnoreCase("date"))
								{
									
									/* Define the time zone for Dallas CST */
						        	ZoneId zoneId = ZoneId.of ( "America/Chicago" );
						        	
						        	/* Convert Epoch Seconds back to a new ZonedDateTime object 
						        	 * First use RandomAccessFile readLong() to retrieve 8-byte
						        	 * integer from table file, then...
						        	 */
						        	//System.out.println(list[z].get(m));
						        	long retreivedEpochSeconds = (long) list[z].get(m);
						        	Instant a = Instant.ofEpochSecond (retreivedEpochSeconds); 
						        	ZonedDateTime zdt2 = ZonedDateTime.ofInstant ( a, zoneId );
						        	
						        	//list[i].add(zdt2.toLocalDate());
									System.out.print(zdt2.toLocalDate() + " ");
								}
								else
								{
									System.out.print(list[z].get(m) + " ");
								}

							}
							System.out.println();
							m++;
						}
					}		
				}
	}
	
	/*
	 * This method returns appropriate ArrayList for the given datatype
	 */
	@SuppressWarnings("rawtypes")
	private ArrayList getArrayList(String data_type) {
		if(data_type.equalsIgnoreCase("int")){
			return new ArrayList<Integer>();
		} 
		else if(data_type.equalsIgnoreCase("tinyint"))
        {			
			return new ArrayList<Integer>();
        }
        else if(data_type.equalsIgnoreCase("smallint"))
        {
        	return new ArrayList<Integer>();
        }
        else if(data_type.equalsIgnoreCase("bigint"))
        {
        	return new ArrayList<Integer>();
        }
        else if(data_type.equalsIgnoreCase("real"))
        {
        	return new ArrayList<Float>();			
        }
        else if(data_type.equalsIgnoreCase("double"))
        {
        	return new ArrayList<Double>();
        }
        else if(data_type.equalsIgnoreCase("datetime"))
        {
        	return new ArrayList<Long>();			
        }
        else if(data_type.equalsIgnoreCase("date"))
        {
        	return new ArrayList<Long>();			
        }
        else if(data_type.equalsIgnoreCase("text"))
        {
        	return new ArrayList<String>();
        } 
        else {
        	return null;
        }
	}

	/*
	 * This method executes SHOW Query
	 */
	public void processShowTableQuery() throws IOException {
		//ArrayList<String> listOfTables = new ArrayList<>();
		seek(8);
		int start = readShort() + 1;
		seek(1);
		int count = readByte();
		
		while(count != 0) {
			seek(start);
			System.out.println(readLine().substring(0, 20).trim());
			start = start - recordLength;
			count--;
		}
	}

	/*
	 * This method executes DELETE Query
	 */
	public void processDeleteQuery(String tableName, String deciding_col, String operator, String comp_val, CreateFile rfTbl, CreateFile rfCol) throws IOException {
		

		if (deciding_col != null) {
			// Find the position where the table name exists in davisbase_columns table
			rfCol.seek(2);
			int position = rfCol.readShort() + 1; // this position will give us the table name
			rfCol.seek(position);
			String referenceTableLine = rfCol.readLine().substring(0, 20);

			//System.out.println("Our table: " + tableName);
			//System.out.println("Reference Table name: " + referenceTableLine);

			int k = 1;
			while (!referenceTableLine.contains(tableName)) { // readLine gives us the table name
				position = position + 84 * (k);
				rfCol.seek(position);
				referenceTableLine = rfCol.readLine().substring(0, 20);
				//System.out.println("Inside Reference Table name: " + referenceTableLine);
			}
			
			String data_type = null;

			String columnNames[] = new String[numberOfColumns];
			Map<String, Integer> mapOfOrdinalPostions = new HashMap<String, Integer>();
			Map<Integer, String> mapOfDataTypes = new HashMap<Integer, String>();
			for (int i = numberOfColumns - 1; i >= 0; i--) {

				// Get column name
				rfCol.seek(position + 20);
				String columnName = rfCol.readLine().substring(0, 20).trim();
				columnNames[i] = columnName;

				// Get ordinal position
				rfCol.seek(position + 62);
				int ordinalPosition = rfCol.readByte();

				// Store it in a map
				mapOfOrdinalPostions.put(columnName, ordinalPosition);

				// Get the data type
				rfCol.seek(position + 40);
				data_type = getSerialCodeAsString(rfCol.readShort());
				//System.out.println(data_type);

				// Store it in a map
				mapOfDataTypes.put(ordinalPosition, data_type);
				position = position + 84;
			}
			int decidingOrdinalPosition = mapOfOrdinalPostions.get(deciding_col);
			String decidingDataType = mapOfDataTypes.get(decidingOrdinalPosition);
			int length = 0;
			for (int i = 1; i < decidingOrdinalPosition; i++) {
				length += getRecordLengthForDataType(mapOfDataTypes.get(i));
				//System.out.println("Length: " + length);
			}
			
			int pageToBeProcessed = 1;
			while(pageToBeProcessed <= countPages) {
				if(pageToBeProcessed == 1) 
				{
					seek(1);
					int numberOfRecords = readByte();
					helperMethodForDelete(pageToBeProcessed, numberOfRecords, length, deciding_col, decidingDataType, operator, comp_val, mapOfDataTypes, mapOfOrdinalPostions);
				}
				else
				{
					seek(pageSize + (pageToBeProcessed - 2)*512 + 1);
					int numberOfRecords = readByte();
					helperMethodForDelete(pageToBeProcessed, numberOfRecords, length, deciding_col, decidingDataType, operator, comp_val, mapOfDataTypes, mapOfOrdinalPostions);
				}
				pageToBeProcessed++;
			}

		} else 
		{
			int end = pageSize + (countPages - 1)*512;
			for (int i = 1; i < end; i++) {
				seek(i);
				writeByte(0);
			}
			recordStartPostion = 8;
			numberOfRecords = 0;
			setLength(pageSize);
			pos = 512;
			countPages = 1;
			
			seek(512);
			writeByte(5);
		}
		System.out.println("Delete Performed successfully!");
	}

	/*
	 * Helper function for DELETE Query
	 */
	public void helperMethodForDelete(int pageToBeProcessed, int numberOfRecords, int length, String deciding_col, String decidingDataType, String operator, String comp_val, Map<Integer, String> mapOfDataTypes, Map<String, Integer> mapOfOrdinalPostions) throws IOException {
			
		int recordStart = 8;
		int start;
		if(pageToBeProcessed == 1) {
			seek(recordStart);
			start = readShort();
			seek(start);
		} else {
			seek(pageSize + (pageToBeProcessed - 2)*512 + recordStart);
			start = readShort();
			seek(start);
		}
		
		for (int i = 0; i < numberOfRecords; i++) {
			boolean flag = false;
			seek(start + length);
			if ("<".equals(operator)) {
				if(decidingDataType.equalsIgnoreCase("int"))
				{
					if (readInt() < Integer.parseInt(comp_val)) 
					{
						makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}
				
				}
				else if(decidingDataType.equalsIgnoreCase("tinyint"))
		        {			
					if (readByte() < Byte.parseByte(comp_val)) 
					{
						makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}
		        }
		        else if(decidingDataType.equalsIgnoreCase("smallint"))
		        {
		        	if (readShort() < Short.parseShort(comp_val)) 
					{
						makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}
		        }
		        else if(decidingDataType.equalsIgnoreCase("bigint"))
		        {
		        	if (readLong() < Long.parseLong(comp_val)) 
					{
						makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}
		        }
		        else if(decidingDataType.equalsIgnoreCase("real"))
		        {
		        	if (readFloat() < Float.parseFloat(comp_val)) 
					{
						makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}			
		        }
		        else if(decidingDataType.equalsIgnoreCase("double"))
		        {
		        	if (readDouble() < Double.parseDouble(comp_val)) 
					{
						makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}
		        }
		        else if(decidingDataType.equalsIgnoreCase("datetime"))
		        {
		        	String dateParams[] = comp_val.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");
	        		
	        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
	        		
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		/* ZonedDateTime toLocalDate() method will display in a simple format */
	        		//System.out.println(zdt.toLocalDate()); 
	        		
	        		/* Convert a ZonedDateTime object to epochSeconds
	        		 * This value can be store 8-byte integer to a binary
	        		 * file using RandomAccessFile writeLong()
	        		 */
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
		        	
		        	if(readLong() < epochSeconds)
		        	{
		        		makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}		
		        }
		        else if(decidingDataType.equalsIgnoreCase("date"))
		        {
		        	String dateParams[] = comp_val.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");
	        		
	        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
	        		
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		/* ZonedDateTime toLocalDate() method will display in a simple format */
	        		//System.out.println(zdt.toLocalDate()); 
	        		
	        		/* Convert a ZonedDateTime object to epochSeconds
	        		 * This value can be store 8-byte integer to a binary
	        		 * file using RandomAccessFile writeLong()
	        		 */
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
		        	
		        	if(readLong() < epochSeconds)
		        	{
		        		makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}					
		        }
		        				
			} 
			else if (">".equals(operator)) 
			{
				if(decidingDataType.equalsIgnoreCase("int"))
				{
					if (readInt() > Integer.parseInt(comp_val)) 
					{
						makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}
				
				}
				else if(decidingDataType.equalsIgnoreCase("tinyint"))
		        {			
					if (readByte() > Byte.parseByte(comp_val)) 
					{
						makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}
		        }
		        else if(decidingDataType.equalsIgnoreCase("smallint"))
		        {
		        	if (readShort() > Short.parseShort(comp_val)) 
					{
						makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}
		        }
		        else if(decidingDataType.equalsIgnoreCase("bigint"))
		        {
		        	if (readLong() > Long.parseLong(comp_val)) 
					{
						makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}
		        }
		        else if(decidingDataType.equalsIgnoreCase("real"))
		        {
		        	if (readFloat() > Float.parseFloat(comp_val)) 
					{
						makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}			
		        }
		        else if(decidingDataType.equalsIgnoreCase("double"))
		        {
		        	if (readDouble() > Double.parseDouble(comp_val)) 
					{
						makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}
		        }
		        else if(decidingDataType.equalsIgnoreCase("datetime"))
		        {
		        	String dateParams[] = comp_val.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");
	        		
	        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
	        		
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		/* ZonedDateTime toLocalDate() method will display in a simple format */
	        		//System.out.println(zdt.toLocalDate()); 
	        		
	        		/* Convert a ZonedDateTime object to epochSeconds
	        		 * This value can be store 8-byte integer to a binary
	        		 * file using RandomAccessFile writeLong()
	        		 */
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
		        	
		        	if(readLong() > epochSeconds)
		        	{
		        		makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}		   		
		        }
		        else if(decidingDataType.equalsIgnoreCase("date"))
		        {
		        	String dateParams[] = comp_val.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");
	        		
	        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
	        		
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		/* ZonedDateTime toLocalDate() method will display in a simple format */
	        		//System.out.println(zdt.toLocalDate()); 
	        		
	        		/* Convert a ZonedDateTime object to epochSeconds
	        		 * This value can be store 8-byte integer to a binary
	        		 * file using RandomAccessFile writeLong()
	        		 */
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
		        	
		        	if(readLong() > epochSeconds)
		        	{
		        		makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}	    			
		      }
			}			
			else 
			{ // = operator
				if(decidingDataType.equalsIgnoreCase("int"))
				{
					if (readInt() == Integer.parseInt(comp_val)) 
					{
						makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}
				
				}
				else if(decidingDataType.equalsIgnoreCase("tinyint"))
		        {			
					if (readByte() == Byte.parseByte(comp_val)) 
					{
						makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}
		        }
		        else if(decidingDataType.equalsIgnoreCase("smallint"))
		        {
		        	if (readShort() == Short.parseShort(comp_val)) 
					{
						makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}
		        }
		        else if(decidingDataType.equalsIgnoreCase("bigint"))
		        {
		        	if (readLong() == Long.parseLong(comp_val)) 
					{
						makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}
		        }
		        else if(decidingDataType.equalsIgnoreCase("real"))
		        {
		        	if (readFloat() == Float.parseFloat(comp_val)) 
					{
						makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}			
		        }
		        else if(decidingDataType.equalsIgnoreCase("double"))
		        {
		        	if (readDouble() == Double.parseDouble(comp_val)) 
					{
						makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}
		        }
		        else if(decidingDataType.equalsIgnoreCase("datetime"))
		        {
		        	String dateParams[] = comp_val.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");
	        		
	        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
	        		
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		/* ZonedDateTime toLocalDate() method will display in a simple format */
	        		//System.out.println(zdt.toLocalDate()); 
	        		
	        		/* Convert a ZonedDateTime object to epochSeconds
	        		 * This value can be store 8-byte integer to a binary
	        		 * file using RandomAccessFile writeLong()
	        		 */
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
		        	
		        	if(readLong() == epochSeconds)
		        	{
		        		makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}					
		        		
		        }
		        else if(decidingDataType.equalsIgnoreCase("date"))
		        {
		        	String dateParams[] = comp_val.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");
	        		
	        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
	        		
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		/* ZonedDateTime toLocalDate() method will display in a simple format */
	        		//System.out.println(zdt.toLocalDate()); 
	        		
	        		/* Convert a ZonedDateTime object to epochSeconds
	        		 * This value can be store 8-byte integer to a binary
	        		 * file using RandomAccessFile writeLong()
	        		 */
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
		        	
		        	if(readLong() == epochSeconds)
		        	{
		        		makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);	
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}					
		        		
		        } 
		        else if (decidingDataType.equalsIgnoreCase("text")) 
		        {
					String stringToBecompared = readLine().substring(0, 20).trim();
					if (stringToBecompared.equals(comp_val)) {
						makeRecordZero(start, start + recordLength, mapOfDataTypes, mapOfOrdinalPostions, pageToBeProcessed);
						flag = true;
						recordStartPostion = recordStartPostion - 2;
					}
				}

			}
			if(!flag) {
				start = start - recordLength;
			} else {
				i--;
			}
		}
	}

	/*
	 * This method makes the record entry zero for the record which is deleted
	 */
	public void makeRecordZero(int start, int end, Map<Integer, String> mapOfDataTypes, Map<String, Integer> mapOfOrdinalPostions, int pageToBeProcessed) throws IOException {
		for (int j = start; j < end; j++) {
			seek(j);
			writeByte(0);
		}
		int address = pos;
		for (int i = 1; i <= numberOfColumns; i++) {
			seek(address);
			if(mapOfDataTypes.get(i).equalsIgnoreCase("int")){
				int temp = readInt();
				seek(start);
				writeInt(temp);
				start = start + 4;
				address = address + 4;
			}else if(mapOfDataTypes.get(i).equalsIgnoreCase("byte"))
	        {			
				int temp = readByte();
				seek(start);
				writeByte(temp);
				start = start + 1;
				address = address + 1;
	        }
			else if(mapOfDataTypes.get(i).equalsIgnoreCase("tinyint"))
	        {			
				int temp = readByte();
				seek(start);
				writeByte(temp);
				start = start + 1;
				address = address + 1;
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("smallint"))
	        {
	        	int temp = readShort();
				seek(start);
				writeInt(temp);
				start = start + 2;
				address = address + 2;
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("bigint"))
	        {
	        	long temp = readLong();
				seek(start);
				writeLong(temp);
				start = start + 8;
				address = address + 8;
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("real"))
	        {
	        	float temp = readFloat();
				seek(start);
				writeFloat(temp);
				start = start + 4;
				address = address + 4;		
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("double"))
	        {
	        	double temp = readDouble();
				seek(start);
				writeDouble(temp);
				start = start + 8;
				address = address + 8;
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("datetime"))
	        {
	        	long temp = readLong();
				seek(start);
				writeLong(temp);
				start = start + 8;
				address = address + 8;	
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("date"))
	        {
	        	//String temp = readLine().substring(0, 20);
	        	long temp = readLong();
				seek(start);
				writeLong(temp);
				start = start + 8;
				address = address + 8;			
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("text"))
	        {
	        	String temp = readLine().substring(0, 20);
	        	seek(start);
				writeBytes(temp);
				start = start + 20;
				address = address + 20;
	        } 
		}
		
		int s = pos;
		for (int j = s; j < (pos+recordLength); j++) {
			seek(j);
			writeByte(0);
		}
		
		int pageCurrent = (int)Math.floor(pos/512);
		int recordStart = 8;
		
		if(pageCurrent == 0) {
			//Make the address of last record copied as zero
			seek(1);
			int numOfRecords = readByte();
			seek(recordStart + (numOfRecords-1)*2);
			writeShort(0);
			
			//Decrementing number of records
			seek(1);
			int updatedNumOfRecords = readByte();
			seek(1);
			writeByte(updatedNumOfRecords-1);
			
			//Update pos and Update the address of latest record inserted
			pos = pos + recordLength;
			seek(2);
			writeShort(pos);
			
		} else { //If current page is other than page 1
			
			//Update pos and Update the address of latest record inserted
			if((pos + recordLength) >= (pageSize + (pageCurrent - 2)*512 + 512)) {
				if(pageCurrent == 2) {
					seek(2);
					pos = readShort();
					
					//Set pointer to right child as zero
					seek(4);
					writeInt(0);
					
					setLength(pageSize);
					countPages--;
				}
				else {
					seek((pageCurrent - 1)* 512 + 2);
					pos = readShort();
					
					//Set pointer to right child as zero
					seek(pageSize + (pageCurrent - 2)*512 + 4);
					writeInt(0);
					
					setLength(pageSize + (pageCurrent - 2)*512);
					countPages--;
				}
			}
			else {
				pos = pos + recordLength;
				
				//Update the address of latest record inserted			
				seek(pageSize + (pageCurrent - 2)*512 + 2);
				writeShort(pos);
				
				//Make the address of last record copied as zero
				seek(pageSize + (pageCurrent - 2)*512 + 1);
				int n = readByte();
				seek(pageSize + (pageCurrent - 2)*512 + recordStart + (n-1)*2);
				writeShort(0);
				
				//Decrementing number of records
				seek(pageSize + (pageCurrent - 2)*512 + 1);
				int updatedNumOfRecords = readByte();
				seek(pageSize + (pageCurrent - 2)*512 + 1);
				writeByte(updatedNumOfRecords-1);
			}
		}
		this.numberOfRecords = calculateNumberOfRecords(countPages);
	}

	/*
	 * This method calculates number of records in a table 
	 */
	public int calculateNumberOfRecords(int countPages) throws IOException {
		int c = 1;
		int numOfRecords = 0;
		while(c <= countPages) {
			if(c == 1) {
				seek(1);
				numOfRecords += readByte();
			} else {
				seek(pageSize + (c - 2)*512  + 1);
				numOfRecords += readByte();
			}
			c++;
		}
		return numOfRecords;
	}

	/*
	 * This method makes record entry zero for table which is dropped
	 */
	private void makeRecordZeroDrop(int start, int end, Map<Integer, String> mapOfDataTypes, Map<String, Integer> mapOfOrdinalPostions) throws IOException {
		for (int j = start; j < end; j++) {
			seek(j);
			writeByte(0);
		}
		seek(2);
		int address = readShort();
		for (int i = 1; i <= numberOfColumns; i++) {
			seek(address);
			if(mapOfDataTypes.get(i).equalsIgnoreCase("int")){
				int temp = readInt();
				seek(start);
				writeInt(temp);
				start = start + 4;
				address = address + 4;
			}else if(mapOfDataTypes.get(i).equalsIgnoreCase("byte"))
	        {			
				int temp = readByte();
				seek(start);
				writeByte(temp);
				start = start + 1;
				address = address + 1;
	        }
			else if(mapOfDataTypes.get(i).equalsIgnoreCase("tinyint"))
	        {			
				int temp = readByte();
				seek(start);
				writeByte(temp);
				start = start + 1;
				address = address + 1;
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("smallint"))
	        {
	        	int temp = readShort();
				seek(start);
				writeInt(temp);
				start = start + 2;
				address = address + 2;
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("bigint"))
	        {
	        	long temp = readLong();
				seek(start);
				writeLong(temp);
				start = start + 8;
				address = address + 8;
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("real"))
	        {
	        	float temp = readFloat();
				seek(start);
				writeFloat(temp);
				start = start + 4;
				address = address + 4;		
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("double"))
	        {
	        	double temp = readDouble();
				seek(start);
				writeDouble(temp);
				start = start + 8;
				address = address + 8;
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("datetime"))
	        {
	        	//String temp = readLine().substring(0, 20);
				long temp = readLong();
	        	seek(start);
				writeLong(temp);
				start = start + 8;
				address = address + 8;		
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("date"))
	        {
	        	//String temp = readLine().substring(0, 20);
				long temp = readLong();
	        	seek(start);
				writeLong(temp);
				start = start + 8;
				address = address + 8;			
	        }
	        else if(mapOfDataTypes.get(i).equalsIgnoreCase("text"))
	        {
	        	String temp = readLine().substring(0, 20);
				seek(start);
				writeBytes(temp);
				start = start + 20;
				address = address + 20;
	        } 
		}
		
		//Make the address of last record as zero
		seek(8 + (numberOfRecords-1)*2);
		writeShort(0);
				
		//Decrementing number of records
		seek(1);
		int updatedNumOfRecords = readByte();
		seek(1);
		writeByte(updatedNumOfRecords-1);
		this.numberOfRecords = updatedNumOfRecords-1;
		
		//Make the last row as zero
		seek(2);
		int add = readShort();
		for (int j = add; j <= (add + recordLength); j++) {
			seek(j);
			writeByte(0);
		}
		
		seek(2);
		int u = readShort() + recordLength;
		seek(2);
		writeShort(u);
	}
	
	/*
	 * This method executes DROP Query
	 */
	public void processDropString(String tableName, CreateFile rfTbl, CreateFile rfCol) throws IOException {
		
		//Find the position where the table name exists in davisbase_table table
		rfTbl.seek(2);
		int rfTblPosition = rfTbl.readShort() + 1; // this position will give us the table name
		rfTbl.seek(rfTblPosition);
		String rfTblReferenceTableLine = rfTbl.readLine().substring(0, 20);
		//System.out.println("Our table: " + tableName);
		//System.out.println("RfTbl Reference Table name: " + rfTblReferenceTableLine);
		
		int m = 1;
		while(!rfTblReferenceTableLine.contains(tableName)) { //readLine gives us the table name	
			rfTblPosition = rfTblPosition + 21*(m);
			rfTbl.seek(rfTblPosition);
			rfTblReferenceTableLine = rfTbl.readLine().substring(0, 20);
			//System.out.println("Inside Reference Table name: " + rfTblReferenceTableLine);
		}
		Map<String, Integer> rfTblMapOfOrdinalPostions = new HashMap<String, Integer>();
		rfTblMapOfOrdinalPostions.put("rowid", 1);
		rfTblMapOfOrdinalPostions.put("table_name", 2);
		Map<Integer, String> rfTblMapOfDataTypes = new HashMap<Integer, String>();
		rfTblMapOfDataTypes.put(1, "byte");
		rfTblMapOfDataTypes.put(2, "text");
		rfTbl.makeRecordZeroDrop(rfTblPosition - 1, (rfTblPosition -1 + rfTbl.recordLength), rfTblMapOfDataTypes, rfTblMapOfOrdinalPostions);
		rfTbl.recordStartPostion = rfTbl.recordStartPostion - 2;
		rfTbl.rfTblPos = rfTbl.rfTblPos + rfTbl.recordLength;
		
		//Find the position where the table name exists in davisbase_columns table
		rfCol.seek(2);
		int rfColPosition = rfCol.readShort() + 1; // this position will give us the table name
		rfCol.seek(rfColPosition);
		String rfColReferenceTableLine = rfCol.readLine().substring(0, 20);
		
		//System.out.println("Our table: " + tableName);
		//System.out.println("RfCol Reference Table name: " + rfColReferenceTableLine);
		
		int k = 1;
		while(!rfColReferenceTableLine.contains(tableName)) { //readLine gives us the table name	
			rfColPosition = rfColPosition + 84*(k);
			rfCol.seek(rfColPosition);
			rfColReferenceTableLine = rfCol.readLine().substring(0, 20);
			//System.out.println("Inside Reference Table name: " + rfColReferenceTableLine);
		}
		Map<String, Integer> rfColMapOfOrdinalPostions = new HashMap<String, Integer>();
		rfColMapOfOrdinalPostions.put("rowid", 1);
		rfColMapOfOrdinalPostions.put("table_name", 2);
		rfColMapOfOrdinalPostions.put("column_name", 3);
		rfColMapOfOrdinalPostions.put("data_type", 4);
		rfColMapOfOrdinalPostions.put("column_key", 5);
		rfColMapOfOrdinalPostions.put("ordinal_position", 6);
		rfColMapOfOrdinalPostions.put("is_nullable", 7);

		Map<Integer, String> rfColMapOfDataTypes = new HashMap<Integer, String>();
		rfColMapOfDataTypes.put(1, "BYTE");
		rfColMapOfDataTypes.put(2, "TEXT");
		rfColMapOfDataTypes.put(3, "TEXT");
		rfColMapOfDataTypes.put(4, "SMALLINT");
		rfColMapOfDataTypes.put(5, "TEXT");
		rfColMapOfDataTypes.put(6, "TINYINT");
		rfColMapOfDataTypes.put(7, "TEXT");
		
		for (int i = 0; i < numberOfColumns; i++) {
			rfCol.makeRecordZeroDrop(rfColPosition - 1, (rfColPosition - 1 + rfCol.recordLength), rfColMapOfDataTypes, rfColMapOfOrdinalPostions);
			rfColPosition = rfColPosition + 84;
			rfCol.recordStartPostion = rfCol.recordStartPostion - 2;
			rfCol.rfColPos = rfCol.rfColPos + rfCol.recordLength;
		}
			
		System.out.println("Drop performed Successfully!");
	}

	/*
	 * This method executes UPDATE Query
	 */
	public void processUpdateString(String tableName, String columnToBeUpdated, String valueToBeSet, String deciding_col, String operator, String comp_val, CreateFile rfTbl, CreateFile rfCol) throws IOException {
		rfCol.seek(2);
		int rfColPosition = rfCol.readShort() + 1; // this position will give us the table name
		rfCol.seek(rfColPosition);
		String rfColReferenceTableLine = rfCol.readLine().substring(0, 20);

		//System.out.println("Our table: " + tableName);
		//System.out.println("RfCol Reference Table name: " + rfColReferenceTableLine);
		
		int k = 1;
		while(!(rfColReferenceTableLine.contains(tableName))) { //readLine gives us the table name	
			rfColPosition = rfColPosition + 84*(k);
			rfCol.seek(rfColPosition);
			rfColReferenceTableLine = rfCol.readLine().substring(0, 20);
			//System.out.println("Inside Reference Table name: " + rfColReferenceTableLine);
		}
		
		String data_type = null;

		String columnNames[] = new String[numberOfColumns];
		Map<String, Integer> mapOfOrdinalPostions = new HashMap<String, Integer>();
		Map<Integer, String> mapOfDataTypes = new HashMap<Integer, String>();
		for (int i = numberOfColumns - 1; i >= 0; i--) {

			// Get column name
			rfCol.seek(rfColPosition + 20);
			String columnName = rfCol.readLine().substring(0, 20).trim();
			columnNames[i] = columnName;

			// Get ordinal position
			rfCol.seek(rfColPosition + 62);
			int ordinalPosition = rfCol.readByte();

			// Store it in a map
			mapOfOrdinalPostions.put(columnName, ordinalPosition);

			// Get the data type
			rfCol.seek(rfColPosition + 40);
			data_type = getSerialCodeAsString(rfCol.readShort());
			//System.out.println(data_type);

			// Store it in a map
			mapOfDataTypes.put(ordinalPosition, data_type);
			rfColPosition = rfColPosition + 84;
		}
		
		int columnToBeUpdatedOrdinalPosition = mapOfOrdinalPostions.get(columnToBeUpdated);
		String columnToBeUpdatedDataType = mapOfDataTypes.get(columnToBeUpdatedOrdinalPosition);
		
		int lengthUpdate = 0;
		for (int i = 1; i < columnToBeUpdatedOrdinalPosition; i++) {
			lengthUpdate += getRecordLengthForDataType(mapOfDataTypes.get(i));
			//System.out.println("Length: " + lengthUpdate);
		}
		
		if(deciding_col != null) {
			int decidingOrdinalPosition = mapOfOrdinalPostions.get(deciding_col);
			String decidingDataType = mapOfDataTypes.get(decidingOrdinalPosition);
			int lengthCondition = 0;
			for (int i = 1; i < decidingOrdinalPosition; i++) {
				lengthCondition += getRecordLengthForDataType(mapOfDataTypes.get(i));
				//System.out.println("Length: " + lengthCondition);
			}
			
			
			int pageToBeProcessed = 1;
			while(pageToBeProcessed <= countPages) {
				if(pageToBeProcessed == 1) 
				{
					seek(1);
					int numberOfRecords = readByte();
					helperMethodForUpdate(lengthUpdate, lengthCondition, pageToBeProcessed, numberOfRecords, deciding_col, decidingDataType, columnToBeUpdatedDataType, operator, comp_val, valueToBeSet, rfTbl, rfCol);
				}
				else
				{
					seek(pageSize + (pageToBeProcessed - 2)*512 + 1);
					int numberOfRecords = readByte();
					helperMethodForUpdate(lengthUpdate, lengthCondition, pageToBeProcessed, numberOfRecords, deciding_col, decidingDataType, columnToBeUpdatedDataType, operator, comp_val, valueToBeSet, rfTbl, rfCol);
				}
				pageToBeProcessed++;
			}
				
		}
		else 
		{
			int pageToBeProcessed = 1;
			while(pageToBeProcessed <= countPages) {
				if(pageToBeProcessed == 1) 
				{
					seek(8);
					int startUpdate = readShort() + lengthUpdate;
					seek(1);
					int numberOfRecords = readByte();
					for (int j = 0; j < numberOfRecords; j++) {
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
						startUpdate = startUpdate - recordLength;
					}
					
				}
				else
				{
					seek(pageSize + (pageToBeProcessed - 2)*512 + 8);
					int startUpdate = readShort() + lengthUpdate;
					
					seek(pageSize + (pageToBeProcessed - 2)*512 + 1);
					int numberOfRecords = readByte();
					
					for (int j = 0; j < numberOfRecords; j++) {
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
						startUpdate = startUpdate - recordLength;
					}
			
				}
				pageToBeProcessed++;
			}
			
			
			
		}
		
		System.out.println("Update Performed Successfully!");
	}
	
	/*
	 * Helper function for UPDATE Query
	 */
	private void helperMethodForUpdate(int lengthUpdate, int lengthCondition, int pageToBeProcessed, int numberOfRecords, String deciding_col, String decidingDataType, String columnToBeUpdatedDataType, String operator,
			String comp_val, String valueToBeSet, CreateFile rfTbl, CreateFile rfCol) throws IOException {
		
		
		int recordStart = 8;
		int startUpdate;
		int startCondition;
		if(pageToBeProcessed == 1) {
			seek(recordStart);
			startUpdate = readShort() + lengthUpdate;
			seek(recordStart);
			startCondition = readShort() + lengthCondition;
		} else {
			seek(pageSize + (pageToBeProcessed - 2)*512 + recordStart);
			startUpdate = readShort() + lengthUpdate;
			seek(pageSize + (pageToBeProcessed - 2)*512 + recordStart);
			startCondition = readShort() + lengthCondition;
		}
		
		for (int j = 0; j < numberOfRecords; j++) {
			if ("<".equals(operator)) 
			{
				seek(startCondition);
				if(decidingDataType.equalsIgnoreCase("int")) 
				{
					if (readInt() < Integer.parseInt(comp_val)) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("byte")) 
				{
					if (readByte() < Byte.parseByte(comp_val)) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("tinyint")) 
				{
					if (readByte() < Byte.parseByte(comp_val)) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("smallint")) 
				{
					if (readShort() < Short.parseShort(comp_val)) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("bigint")) 
				{
					if (readLong() < Long.parseLong(comp_val)) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}				
				else if(decidingDataType.equalsIgnoreCase("double")) 
				{
					if (readDouble() < Double.parseDouble(comp_val)) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("datetime"))
				{
					String dateParams[] = comp_val.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");
	        		
	        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
	        		
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		/* ZonedDateTime toLocalDate() method will display in a simple format */
	        		//System.out.println(zdt.toLocalDate()); 
	        		
	        		/* Convert a ZonedDateTime object to epochSeconds
	        		 * This value can be store 8-byte integer to a binary
	        		 * file using RandomAccessFile writeLong()
	        		 */
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
	        		
	        		if (readLong() < epochSeconds) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("date"))
				{
					String dateParams[] = comp_val.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");
	        		
	        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
	        		
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		/* ZonedDateTime toLocalDate() method will display in a simple format */
	        		//System.out.println(zdt.toLocalDate()); 
	        		
	        		/* Convert a ZonedDateTime object to epochSeconds
	        		 * This value can be store 8-byte integer to a binary
	        		 * file using RandomAccessFile writeLong()
	        		 */
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
	        		
	        		if (readLong() < epochSeconds) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}
			} 
			else if (">".equals(operator)) 
			{
				seek(startCondition);
				if(decidingDataType.equalsIgnoreCase("int")) 
				{
					if (readInt() > Integer.parseInt(comp_val)) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("byte")) 
				{
					if (readByte() > Byte.parseByte(comp_val)) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("tinyint")) 
				{
					if (readByte() > Byte.parseByte(comp_val)) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("smallint")) 
				{
					if (readShort() > Short.parseShort(comp_val)) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("bigint")) 
				{
					if (readLong() > Long.parseLong(comp_val)) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("double")) 
				{
					if (readDouble() > Double.parseDouble(comp_val)) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("datetime"))
				{
					String dateParams[] = comp_val.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");
	        		
	        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
	        		
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		/* ZonedDateTime toLocalDate() method will display in a simple format */
	        		//System.out.println(zdt.toLocalDate()); 
	        		
	        		/* Convert a ZonedDateTime object to epochSeconds
	        		 * This value can be store 8-byte integer to a binary
	        		 * file using RandomAccessFile writeLong()
	        		 */
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
	        		
	        		if (readLong() > epochSeconds) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("date"))
				{
					String dateParams[] = comp_val.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");
	        		
	        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
	        		
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		/* ZonedDateTime toLocalDate() method will display in a simple format */
	        		//System.out.println(zdt.toLocalDate()); 
	        		
	        		/* Convert a ZonedDateTime object to epochSeconds
	        		 * This value can be store 8-byte integer to a binary
	        		 * file using RandomAccessFile writeLong()
	        		 */
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
	        		
	        		if (readLong() > epochSeconds) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}
			} 
			else 
			{ // = operator
				seek(startCondition);
				if(decidingDataType.equalsIgnoreCase("int")) 
				{
					int x = readInt();
					if (x == Integer.parseInt(comp_val)) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("byte")) 
				{
					if (readByte() == Byte.parseByte(comp_val)) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("tinyint")) 
				{
					if (readByte() == Byte.parseByte(comp_val)) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("smallint")) 
				{
					if (readShort() == Short.parseShort(comp_val)) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("bigint")) 
				{
					if (readLong() == Long.parseLong(comp_val)) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("double")) 
				{
					if (readDouble() < Double.parseDouble(comp_val)) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("datetime"))
				{
					String dateParams[] = comp_val.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");
	        		
	        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
	        		
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		/* ZonedDateTime toLocalDate() method will display in a simple format */
	        		//System.out.println(zdt.toLocalDate()); 
	        		
	        		/* Convert a ZonedDateTime object to epochSeconds
	        		 * This value can be store 8-byte integer to a binary
	        		 * file using RandomAccessFile writeLong()
	        		 */
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
	        		
	        		if (readLong() == epochSeconds) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}
				else if(decidingDataType.equalsIgnoreCase("date"))
				{
					String dateParams[] = comp_val.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");
	        		
	        		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
	        		
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		/* ZonedDateTime toLocalDate() method will display in a simple format */
	        		//System.out.println(zdt.toLocalDate()); 
	        		
	        		/* Convert a ZonedDateTime object to epochSeconds
	        		 * This value can be store 8-byte integer to a binary
	        		 * file using RandomAccessFile writeLong()
	        		 */
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
	        		
	        		if (readLong() == epochSeconds) 
					{
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}
				else if (decidingDataType.equalsIgnoreCase("text")) 
				{
					String stringToBecompared = readLine().substring(0, 20).trim();
					if (stringToBecompared.equals(comp_val)) {
						seek(startUpdate);
						updateValue(startUpdate, columnToBeUpdatedDataType, valueToBeSet);
					}
				}

			}
			startCondition = startCondition - recordLength;
			startUpdate = startUpdate - recordLength;
		}			
		
		
	}

	/*
	 * This method writes value based on the data type
	 */
	public void updateValue(int startUpdate, String columnToBeUpdatedDataType, String valueToBeSet) throws NumberFormatException, IOException {
		if(columnToBeUpdatedDataType.equalsIgnoreCase("int"))
		{
			writeInt(Integer.parseInt(valueToBeSet));
		}
		else if(columnToBeUpdatedDataType.equalsIgnoreCase("byte"))
        {	
			writeByte(Byte.parseByte(valueToBeSet));
        }
		else if(columnToBeUpdatedDataType.equalsIgnoreCase("tinyint"))
        {	
			writeByte(Byte.parseByte(valueToBeSet));
        }
        else if(columnToBeUpdatedDataType.equalsIgnoreCase("smallint"))
        {
			writeInt(Short.parseShort(valueToBeSet));
        }
        else if(columnToBeUpdatedDataType.equalsIgnoreCase("bigint"))
        {
			writeLong(Long.parseLong(valueToBeSet));
        }
        else if(columnToBeUpdatedDataType.equalsIgnoreCase("real"))
        {
			writeFloat(Float.parseFloat(valueToBeSet));		
        }
        else if(columnToBeUpdatedDataType.equalsIgnoreCase("double"))
        {
			writeDouble(Double.parseDouble(valueToBeSet));
        }
        else if(columnToBeUpdatedDataType.equalsIgnoreCase("datetime"))
        {
        	
        	String dateParams[] = valueToBeSet.split("-");
    		ZoneId zoneId = ZoneId.of( "America/Chicago");
    		
    		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
    		
    		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
    		/* ZonedDateTime toLocalDate() method will display in a simple format */
    		//System.out.println(zdt.toLocalDate()); 
    		
    		/* Convert a ZonedDateTime object to epochSeconds
    		 * This value can be store 8-byte integer to a binary
    		 * file using RandomAccessFile writeLong()
    		 */
    		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
    		writeLong ( epochSeconds );		
        }
        else if(columnToBeUpdatedDataType.equalsIgnoreCase("date"))
        {
        	String dateParams[] = valueToBeSet.split("-");
    		ZoneId zoneId = ZoneId.of( "America/Chicago");
    		
    		/* Convert date and time parameters for 1974-05-27 to a ZonedDateTime object */
    		
    		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
    		/* ZonedDateTime toLocalDate() method will display in a simple format */
    		//System.out.println(zdt.toLocalDate()); 
    		
    		/* Convert a ZonedDateTime object to epochSeconds
    		 * This value can be store 8-byte integer to a binary
    		 * file using RandomAccessFile writeLong()
    		 */
    		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
    		writeLong ( epochSeconds );				
        }
        else if(columnToBeUpdatedDataType.equalsIgnoreCase("text"))
        {
        	for (int i = startUpdate; i < (startUpdate+20); i++) {
        		writeByte(0);
            	seek(i);
			}
        	seek(startUpdate);
			writeBytes(valueToBeSet);
        } 
		
	}
	
	/*
	 * This method calculates number of columns in a table and also record length of a record in a table	
	 */
	public int[] calculateNumberOfColumnsAndRecordLength(String tableName) throws IOException {
		seek(2);
		int position = readShort() + 1; // this position will give us the table name
		seek(position);
		String referenceTableLine = readLine().substring(0, 20);
		int k = 1;
		int numberOfColumns = 0;
		int recordLength = 0;
		while(!referenceTableLine.contains(tableName)) {
			position = position + 84*(k);
			seek(position);
			referenceTableLine = readLine().substring(0, 20);
		}
		
		while(referenceTableLine.contains(tableName)) { //readLine gives us the table name	
			numberOfColumns++;
			seek(position+40);
			recordLength += getRecordLengthForDataType(getSerialCodeAsString(readShort()));
			position = position + 84*(k);
			seek(position);
			referenceTableLine = readLine().substring(0, 20);
		}
		int values[] = new int[2];
		values[0] = numberOfColumns;
		values[1] = recordLength;
		return values;
		
	}

	/*
	 * This method checks if table already exists in database
	 */
	public boolean checkIfTableAlreadyExists(String tableName) throws IOException {
		seek(1);
		int numberOfRecords = readByte();
		
		seek(2);
		int rfTblPosition = readShort() + 1; // this position will give us the table name
		seek(rfTblPosition);
		String rfTblReferenceTableLine = readLine().substring(0, 20);
		int m = 1;
		int i;
		for (i = 0; i < (numberOfRecords-2); i++) {
			if(!rfTblReferenceTableLine.contains(tableName)) { 
				rfTblPosition = rfTblPosition + 21*(m);
				seek(rfTblPosition);
				rfTblReferenceTableLine = readLine().substring(0, 20);
			}
			else 
				break;
		}
		
		if(i == (numberOfRecords-2))
			return false;
		else
			return true;
	}

}
