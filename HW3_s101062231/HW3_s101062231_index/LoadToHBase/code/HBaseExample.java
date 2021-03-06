package hBaseExample;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.Arrays;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.lang.reflect.InvocationTargetException;

public class HBaseExample {

	/* API can be found here:
		https://hbase.apache.org/apidocs/
	*/

	private static Configuration conf;
	private static Connection connection;
	private static Admin admin;
	
	// A buffer to tempory store for put request, used to speed up the process of putting records into hbase
	private static List<Put> putList;
	private static int listCapacity = 100000;
	
	// TODO: Set up your tableName and columnFamilies in the table
	//private static String[] tableName = {"s101062231:Inverted_Index","s101062231:PageRank"};
	//private static String[] tableColFamilies = {"Info"};

	public static void createTable(String tableName, String[] colFamilies) throws Exception {
		// Instantiating table descriptor class
		TableName hTableName = TableName.valueOf(tableName);
		if (admin.tableExists(hTableName)) {
			System.out.println(tableName + " : Table already exists!");
		} else {
			HTableDescriptor tableDescriptor = new HTableDescriptor(hTableName);
			// TODO: Adding column families to table descriptor
			for (String cf : colFamilies) {
				tableDescriptor.addFamily(new HColumnDescriptor(cf));
				// tableDescriptor.addFamily(new HColumnDescriptor(...));
			}
			// TODO: Admin creates table by HTableDescriptor instance
			System.out.println("Creating table: " + tableName + "...");
			admin.createTable(tableDescriptor); 
			System.out.println("Table created");
		}
	}
	
	public static void removeTable(String tableName) throws Exception {
		TableName hTableName = TableName.valueOf(tableName);
		if (!admin.tableExists(hTableName)) {
			System.out.println(tableName + ": Table does not exist!");
		} else {
			System.out.println("Deleting table: " + tableName + "...");
			// TODO: disable & drop table
			admin.disableTable(hTableName);
			admin.deleteTable(hTableName);
			System.out.println("Table deleted");
		}
	}
	
	public static void addRecordToPutList(String rowKey, String colFamily,
			String qualifier, String value) throws Exception {
		// TODO: use Put to wrap information and put it to PutList.
		//HTable table = new HTable(conf, tableName);
		 Put put = new Put(Bytes.toBytes(rowKey));
//System.out.println("Value: " + value);
//System.out.println("*************************");
         put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
					
         //  table.put(put);
		// Put put = new Put(...);
		// put.addColumn(...);
		putList.add(put);
	}
	
	public static void addRecordToHBase(String tableName) throws Exception {
		// TODO: dump things from memory (PutList) to HBaseConfiguration
		Table table = connection.getTable(TableName.valueOf(tableName));
		//for(Put p:putList){
		System.out.println("put start!");
			table.put(putList);
		System.out.println("put over!");
		//}
		putList.clear();
		// Table table = ...
		// table.put(...)
		// putList.clear();
	}
	
	public static void deleteRecord(String tableName, String rowKey) throws Exception {
		Table table = connection.getTable(TableName.valueOf(tableName));
		//table.delete()
		// TODO use Delete to wrap key information and use Table api to delete it.
	}




	private static void storeInvertedTableToHBase(String inputPath) throws IOException {
		try{
			String[] tableInvertedColFamilies = {"Inverted"};
			conf = HBaseConfiguration.create();
			connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();
			System.out.println("fetching: " + "Inverted_Index" + "...");

			removeTable("s101062231:Inverted_Index");
			createTable("s101062231:Inverted_Index", tableInvertedColFamilies);

			//File file = new File(inputPath);
			//BufferedReader br = new BufferedReader(new FileReader(file));
			int linecount = 0;
			String line = null;
			putList = new ArrayList<Put>(listCapacity);
			
			File file = new File(inputPath);     
			BufferedInputStream fis = new BufferedInputStream(new FileInputStream(file));      
    			BufferedReader br = new BufferedReader(new InputStreamReader(fis,"utf-8"),15*1024*1024);
			StringBuffer sb = new StringBuffer(); 
			String tok[] = new String[5];
			String wordWithDf = new String();
			String tok2[] = new String[5];
			String word = new String();
			String df = new String();
			//String invertedInfo = new String();
			int invertedIndexStart = 0;
			int invertedIndexEnd = 0;
			StringBuffer sb2 = new StringBuffer();
			while (null != (line = br.readLine())) {
				//sb = sb.append(br.readLine());
				//line = sb.toString();
				if (0 == linecount % 100000) System.out.println(linecount + " lines added to hbase.");
				//System.out.println(line);
				// TODO : Split the content of a line and store it to hbase
				
				
				//for Inverted Index
				//input ex: gypsy   2<maindiv>Growel's 101<div>1.0<div>[85920942]<maindiv>Wikipedia:WikiProject Spam/LinkReports/sydroger.blogspot.com<div>2.0<div>[66489813,66490285]
				//key     Inverted[df,info]
				//{word}	{df},{doc tf [XXX,XXX];doc tf [XXXX,XXX]}

				//String tok[] = line.split("<maindiv>");
				//String wordWithDf = tok[0];
				//String tok2[] = wordWithDf.split("\t");
				//
				//
				Arrays.fill(tok, null);
				Arrays.fill(tok2, null);
				wordWithDf = "";

				tok = line.split("<maindiv>");
				wordWithDf = tok[0];
				tok2 = wordWithDf.split("\t");	
							
				word = "";
				df = "";
	
				word = tok2[0];
				df = tok2[1];
				invertedIndexStart = 0;
				invertedIndexStart = 0;
				invertedIndexStart = line.indexOf("<maindiv>");
				invertedIndexStart = invertedIndexStart + 9;
				invertedIndexEnd = line.length();
				//invertedInfo = "";
				//sb2.delete(0,sb2.length());
				//String invertedInfo = line.substring(invertedIndexStart,invertedIndexEnd);
				//sb2.append(line.substring(invertedIndexStart,invertedIndexEnd));
				//invertedInfo = sb2.toString();
				//System.out.println("********************************");
				//System.out.println(word);
				// TODO : Add the record to corresponding hbase table
				//addRecordToPutList(String rowKey, String colFamily,String qualifier, String value)
				double dfTest = Double.parseDouble(df);
				if(dfTest > 5000){
				//System.out.println("********************************");
					//System.out.println(dfTest);
					line = "";
					//sb = sb.delete(0, sb.length());
					continue;
				}
				addRecordToPutList(word,"Inverted","word",word);
				addRecordToPutList(word,"Inverted","df",df);
				//addRecordToPutList(word,"Inverted","invertedInfo",invertedInfo);
				addRecordToPutList(word,"Inverted","invertedInfo",new String(line.substring(invertedIndexStart,invertedIndexEnd)));
				// if capacity of our putList buffer is reached, dump them into HBase
				if (putList.size() == listCapacity) {
					addRecordToHBase("s101062231:Inverted_Index");
				}
				++linecount;
				line = "";
				//sb = sb.delete(0, sb.length());
			}
			System.out.println(linecount + " lines added to hbase.");
			// dump remaining contents into HBase
			
			System.out.println("dump remaing!");
addRecordToHBase("s101062231:Inverted_Index");
			System.out.println("inverted over!");
			br.close();
			admin.close();
			connection.close();
			System.out.println(" inverted close!");
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void storePageRankToHBase(String inputPath) throws IOException {
		try{
			String[] tablePageRankColFamilies = {"PageRank"};
			conf = HBaseConfiguration.create();
			connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();
			System.out.println("fetching: " + "PageRank" + "...");

			removeTable("s101062231:PageRank");
			createTable("s101062231:PageRank", tablePageRankColFamilies);

			File file = new File(inputPath);
			BufferedReader br = new BufferedReader(new FileReader(file));
			int linecount = 0;
			String line = null;
			putList = new ArrayList<Put>(listCapacity);
			while (null != (line = br.readLine())) {
				if (0 == linecount % 100000) System.out.println(linecount + " lines added to hbase.");
				//System.out.println(line);
				// TODO : Split the content of a line and store it to hbase

				String tok[] = line.split("\t");
				String page = tok[0];
				String pageRank = tok[1];
				// TODO : Add the record to corresponding hbase table
				//addRecordToPutList(String rowKey, String colFamily,String qualifier, String value)	
				addRecordToPutList(page,"PageRank","page",page);
				addRecordToPutList(page,"PageRank","pageRank",pageRank);


				// if capacity of our putList buffer is reached, dump them into HBase
				if (putList.size() == listCapacity) {
					addRecordToHBase("s101062231:PageRank");
				}
				++linecount;
			}
			System.out.println(linecount + " lines added to hbase.");
			// dump remaining contents into HBase
			addRecordToHBase("s101062231:PageRank");
			br.close();
			admin.close();
			connection.close();
		}catch (Exception e) {
			e.printStackTrace();
		}
		
	}	
	public static void main(String[] args) {
		try{
			storeInvertedTableToHBase(args[0]);
			storePageRankToHBase(args[1]);
		}catch (Exception e) {
			e.printStackTrace();
		}

	}



	/*public static void main(String[] args){
		
		try {
			// Instantiating hbase connection
			conf = HBaseConfiguration.create();
			connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();
			for (int curTableID = 0; curTableID < args.length; ++curTableID) {
				System.out.println("fetching " + args[0] + "...");
				// remove the old table on hbase
				removeTable(tableName[curTableID]);
				// create a new table on hbase
				createTable(tableName[curTableID], tableColFamilies);
				// Read Content from local file
				File file = new File(args[curTableID]);
				BufferedReader br = new BufferedReader(new FileReader(file));
				int linecount = 0;
				String line = null;
				putList = new ArrayList<Put>(listCapacity);
				while (null != (line = br.readLine())) {
					if (0 == linecount % 100000) System.out.println(linecount + " lines added to hbase.");
					//System.out.println(line);
					// TODO : Split the content of a line and store it to hbase
					String tok[] = line.split("\t");
					// TODO : Add the record to corresponding hbase table
					//addRecordToPutList(String rowKey, String colFamily,String qualifier, String value)
					addRecordToPutList(tok[0],tableColFamilies[0],"name",tok[0]);
					addRecordToPutList(tok[0],tableColFamilies[0],"grade",tok[1]);


					//for Inverted Index
					//word df<maindiv>doc<div>tf<div>[XXX,XXX]<maindiv>doc<div>tf<div>[XXXX,XXX]
					//key     info[df,info]
					//{word}	{df},{doc tf [XXX,XXX];doc tf [XXXX,XXX]}


					// if capacity of our putList buffer is reached, dump them into HBase
					if (putList.size() == listCapacity) {
						addRecordToHBase(tableName[curTableID]);
					}
					++linecount;
				}
				System.out.println(linecount + " lines added to hbase.");
				// dump remaining contents into HBase
				addRecordToHBase(tableName[curTableID]);
				br.close();
			}
			// Finalize and close connection to Hbase
			admin.close();
			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}*/
}
