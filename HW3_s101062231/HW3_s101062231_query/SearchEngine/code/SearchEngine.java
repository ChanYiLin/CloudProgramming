package SearchEngine;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.*;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import java.io.IOException;


public class SearchEngine{
	private static Configuration conf;
	private static Connection connection;
	private static Admin admin;
    private static double N;
    private static ArrayList<String> keyVocabs;
	

	public static void main(String[] args)throws IOException {
		int i,j;
		Configuration conf = new Configuration();
	    FileSystem hdfs = FileSystem.get(conf);

        N = rowCount();
        System.out.println("Page Count:" + N);

		System.out.println("************************");
		System.out.println("Search:");

	    Scanner input      = new Scanner(System.in);
	    String  inputStr   = input.nextLine();
	    String  patternStr = "([A-Za-z]+)\\s*\"(.*)\"";
	    Pattern pattern    = Pattern.compile(patternStr);
	    Matcher matcher    = pattern.matcher(inputStr);
	    input.close();
	    
	    // Split str
	    matcher.find();
        String action = matcher.group(1); 
        String params = matcher.group(2);

        keyVocabs = new ArrayList<String>();
        ArrayList<String> andVocabs = new ArrayList<String>();
        ArrayList<String> orVocabs = new ArrayList<String>();
        ArrayList<String> notVocabs = new ArrayList<String>();
        ArrayList<String>    logics = new ArrayList<String>();
        ArrayList<String> searchVocabs = new ArrayList<String>();
    	// Params might be
        // Basic: cat, Advanced: cat or dog

        String[] paramsArr = params.split("\\s");

        andVocabs.add(paramsArr[0]);
        keyVocabs.add(paramsArr[0]);
        searchVocabs.add(paramsArr[0]);
        	
        if (paramsArr.length > 1) {   	
        	for (i = 1; i < paramsArr.length; i+= 2) {
        		if (paramsArr[i].equals("and")) {
	        		andVocabs.add(paramsArr[i+1]);
                    searchVocabs.add(paramsArr[i+1]);
	        	} else if (paramsArr[i].equals("or")){
                    orVocabs.add(paramsArr[i+1]);
                    searchVocabs.add(paramsArr[i+1]);
                }else{
	        		notVocabs.add(paramsArr[i+1]);
	        	}

	        	keyVocabs.add(paramsArr[i+1]);
        		logics.add(paramsArr[i]);
        	}
        }
        int orVocabsLen = orVocabs.size();
        int andVocabsLen = andVocabs.size();
        int notVocabsLen = notVocabs.size();
	    int    logicsLen =    logics.size();
		System.out.println("************************");
		for(i = 0;i < keyVocabs.size();i++){
			System.out.println("search:" + keyVocabs.get(i));
		}
		System.out.println("************************");
		//ex: search cat and dog
		//1: get the cat dog rows from Inverted Table 
		//2: Find the same Page
		//3: calculate the TF-IDF
		//4: list top ten page
		//5: keep the offset info
		//5: get ten page rows from Page Table
		//6: sort and print and read the segment


		//Read the result file
		HashMap<String, Scoreboard> scoreMap = new HashMap<String, Scoreboard>();
		HashMap<String, Scoreboard> term = readFileCreateHash(andVocabsLen, andVocabs);
        scoreMap.putAll(term);



        HashMap<String, Integer> tempMap = new HashMap<String, Integer>();
        
        for (i = 0; i < andVocabsLen; i++) { 
            Scoreboard tValue = scoreMap.get(andVocabs.get(i)); 

            for (TermFileInfo TermFileInfo : tValue.getTermFileInfos()) {
                
                if (tempMap.get(TermFileInfo.getFileName()) == null) {
                    tempMap.put(TermFileInfo.getFileName(), 1);
                } else {
                    tempMap.replace(TermFileInfo.getFileName(), tempMap.get(TermFileInfo.getFileName())+1);
                }
            }
        }


        
        
        HashMap<String, Integer> FinalMap = new HashMap<String, Integer>();
        for (String key : tempMap.keySet()) {
            if (tempMap.get(key) != 0) {   //collect the file that has been referenced
                FinalMap.put(key, tempMap.get(key));
            }
        }
       // System.out.println(FinalMap);
        
        
        HashMap<String, TermFileInfo> tValueMap = new HashMap<String, TermFileInfo>();
        HashMap<String, Double>     weights = new HashMap<String, Double>();

        for (String vocab : searchVocabs) {
            for (TermFileInfo TermFileInfo : scoreMap.get(vocab).getTermFileInfos()) {

                for (String key : FinalMap.keySet()) {
                
                    if (TermFileInfo.getFileName().equals(key)) {         
                        if (tValueMap.get(key) == null) { 
                            tValueMap.put(key, TermFileInfo);
                        } else {
                            TermFileInfo tmp = tValueMap.get(key);
                            tmp.setScore(tmp.getScore() + TermFileInfo.getScore());
                            tmp.appendOffset(TermFileInfo.getOffset());
                            tValueMap.replace(key, tmp);
                        }
                        
                        if (weights.get(key) == null) { //update weights 裡關於這個file的分數
                            weights.put(key, TermFileInfo.getScore());
                        } else {
                            weights.replace(key, weights.get(key) + TermFileInfo.getScore());
                        }
                    }
                    
                }
            }
        }
        
        //TreeMap<Double, TermFileInfo> pairMap = createPairMap(weights, tValueMap);
        //NavigableMap<Double, TermFileInfo> nmap = pairMap.descendingMap();
        Map<TermFileInfo,Double> pairMap = createPairMap(weights, tValueMap);

        List<Map.Entry<TermFileInfo,Double>> mapInvertedList =
                new ArrayList<Map.Entry<TermFileInfo,Double>>(pairMap.entrySet());
        Collections.sort(mapInvertedList,new Comparator<Map.Entry<TermFileInfo,Double>> (){
            @Override
            public int compare(Map.Entry<TermFileInfo,Double> o1,
                    Map.Entry<TermFileInfo,Double> o2) {
                
                //return o1.getValue() - o2.getValue();

                //return o2.getValue() - o1.getValue();
                /*if(o2.getValue()>o1.getValue()){
                    return 1;
                }else if(o2.getValue()==o1.getValue()){
                    return 0;
                }else{
                    return -1;
                }*/
                return Double.compare(o2.getValue(),o1.getValue()); 
            }
        });



        conf = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
        Table table = connection.getTable(TableName.valueOf("s101062231:PageRank"));
        // Get the page Results
        Map<String, Double> pagePairMap = new HashMap<String, Double>();
        //Map<String,Integer> map = new HashMap<String,Integer>();
        //TreeMap<Double,String> pagePairMap = new TreeMap<Double, String>();
        //HashMap<String, TermFileInfo> lookUpMap = new HashMap<String, TermFileInfo>();
        //map.put("A", 34);
        int Len = (mapInvertedList.size() < 10) ? mapInvertedList.size() : 10;

        for (i = 0; i < Len; i++) {
            Map.Entry<TermFileInfo,Double> xInverted = mapInvertedList.get(i);
            //Entry<Double, TermFileInfo> entry    = nmap.pollFirstEntry();
            //String                    fileName = entry.getValue().getFileName();
            String                      page = xInverted.getKey().getFileName();

            Double                    score = xInverted.getValue();
            
            Get get = new Get(page.getBytes());
            Result res = table.get(get);
            String rankStr = Bytes.toString(res.getValue(Bytes.toBytes("PageRank"), Bytes.toBytes("pageRank")));
            double rank = Double.parseDouble(rankStr);
            //System.out.println("last step here pageRank: " + rank);
            pagePairMap.put(page,rank);

            //Configuration conf = new Configuration();
            /*FileSystem fs = FileSystem.get(conf);
            
            Path inFile = new Path(INPUT_DIR + "/" + fileName);
            if (!fs.exists(inFile))
                System.out.println("Input file not found");
            FSDataInputStream in = fs.open(inFile);

            System.out.println("Rank " + Integer.toString(i+1) + "\t" + fileName + "score = " + score);
            System.out.println("************************");
            
            int len = entry.getValue().getOffset().size();
            for (int j = 0; j < len; j++) {
                // Jump to that position in O(1);
                //Text line = new Text();
                System.out.println("[offest = " + entry.getValue().getOffset().get(j) + "]:");
                System.out.print("=>  ");
                byte[] buffer = new byte[30];
                //in.seek(entry.getValue().getOffset().get(j)-10);
                //LineReader reader = new LineReader(in); 

                in.read(entry.getValue().getOffset().get(j)-10,buffer,0,30);
                String s1 = new String(buffer);
                System.out.println(s1);
            }
            
            System.out.println("************************");*/
        }     
        //Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);   
        Path inFile = new Path("hdfs:///shared/HW2/sample-in/input-1G");   //***change here*****/
        if (!fs.exists(inFile))
            System.out.println("Input file not found");
        FSDataInputStream in = fs.open(inFile);
        //pagePairMap = entriesSortedByValues(pagePairMap);
        //NavigableMap<Double, String> pagenmap = pagePairMap.descendingMap();
        List<Map.Entry<String, Double>> mapList =
                new ArrayList<Map.Entry<String, Double>>(pagePairMap.entrySet());
        Collections.sort(mapList,new Comparator<Map.Entry<String, Double>> (){
            @Override
            public int compare(Map.Entry<String, Double> o1,
                    Map.Entry<String, Double> o2) {
                
                //return o1.getValue() - o2.getValue();

                //return o2.getValue() - o1.getValue();
               /* if(o2.getValue()>o1.getValue()){
                    return 1;
                }else if(o2.getValue()==o1.getValue()){
                    return 0;
                }else{
                    return -1;
                }*/
                   return Double.compare(o2.getValue(),o1.getValue()); 

            }
        });




        Len = (mapList.size() < 10) ? mapList.size() : 10;
        for(i = 0;i < Len;i++){
            Map.Entry<String, Double> x = mapList.get(i);
            //Entry<Double, String> entry    = pagenmap.pollFirstEntry();
            String                page     = x.getKey();
            Double                pageRank = x.getValue();

            TermFileInfo tmp = tValueMap.get(page);
            System.out.println("Rank " + Integer.toString(i+1) + "\t" + page + "\tscore = " + pageRank);
            int len = tmp.getOffset().size();
            if(len > 3)
                len = 3;
            System.out.println("************************");
            for(j = 0;j < len;j++){
                System.out.println("[offest = " + tmp.getOffset().get(j) + "]:");
                System.out.print("=>  ");
                byte[] buffer = new byte[2000];
                in.read(tmp.getOffset().get(j)+500,buffer,0,2000);
                String s1 = new String(buffer);
                System.out.println(s1);

            }

        }
        System.out.println("************************");
	}


	public static HashMap<String, Scoreboard> readFileCreateHash(int andVocabsLen, ArrayList<String> andVocabs) throws NumberFormatException, IOException {
        int i,j;
        HashMap<String, Scoreboard> scoreMap = new HashMap<String, Scoreboard>();

		conf = HBaseConfiguration.create();
		connection = ConnectionFactory.createConnection(conf);
		admin = connection.getAdmin();


        //example: gypsy   2    Growel's 101<div>1.0<div>[85920942]<maindiv>Wikipedia:WikiProject Spam/LinkReports/sydroger.blogspot.com<div>2.0<div>[66489813,66490285]
		
		//1: get the cat dog rows from Inverted Table 
		Table table = connection.getTable(TableName.valueOf("s101062231:Inverted_Index"));
		for(i = 0;i < andVocabsLen;i++){
			Get get = new Get(andVocabs.get(i).getBytes());
			Result res = table.get(get);
			if (!res.isEmpty()) {
                String word = andVocabs.get(i);
                String wordTest = Bytes.toString(res.getValue(Bytes.toBytes("Inverted"), Bytes.toBytes("word")));
                //System.out.println("get word from HBase: " + wordTest);
                String dfString = Bytes.toString(res.getValue(Bytes.toBytes("Inverted"), Bytes.toBytes("df")));
                Double df = Double.parseDouble(dfString);
                String invertedInfo  = Bytes.toString(res.getValue(Bytes.toBytes("Inverted"), Bytes.toBytes("invertedInfo")));
                //System.out.println("get invertedInfo  from HBase: " + invertedInfo);
				// Collect TermFileInfos
                ArrayList<TermFileInfo> termFileInfos = new ArrayList<TermFileInfo>();
                //invertedInfo = Growel's 101<div>1.0<div>[85920942]<maindiv>Wikipedia:WikiProject Spam/LinkReports/sydroger.blogspot.com<div>2.0<div>[66489813,66490285]
                String invertedInfoArr[] = invertedInfo.split("<maindiv>");
                //invertedInfoArr = Growel's 101<div>1.0<div>[85920942], Wikipedia:WikiProject Spam/LinkReports/sydroger.blogspot.com<div>2.0<div>[66489813,66490285]
                for(j = 0;j < invertedInfoArr.length;j++){
                   // System.out.println("invertedInfoArr [" +j+"]"+ invertedInfoArr[j]);
                    TermFileInfo termFileInfo = createTermFileInfo(invertedInfoArr[j],df);
                   // System.out.println("termFileInfo.getfileName" + termFileInfo.getFileName());
                    termFileInfos.add(termFileInfo);
                }
                Scoreboard sValue = new Scoreboard(termFileInfos);
                scoreMap.put(word, sValue);    
                //pr = new PageRank(key,Bytes.toDouble(r.getValue(Bytes.toBytes("pagerank"), Bytes.toBytes("score"))));
			}
		}
		

		/*Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path inFile = new Path(dir);
		if (!fs.exists(inFile))
  			System.out.println("Input file not found");
  		FSDataInputStream in = fs.open(inFile);


  		Text line = new Text();
		LineReader reader = new LineReader(in); //read line by line

        
        while (reader.readLine(line) > 0) { 

        	String str = line.toString();
            String[] strArr = str.split(";");


            String[] strPart   = strArr[0].split("[\\s]+");
            String   word      = strPart[0];
            
            // Collect TermFileInfos
            ArrayList<TermFileInfo> TermFileInfos = new ArrayList<TermFileInfo>();
            int len = strArr.length;        
            for (int i = 1; i < len; i++) {
                TermFileInfo TermFileInfo = createTermFileInfo(strArr[i]);
                TermFileInfos.add(TermFileInfo);
            }
        
            Scoreboard sValue = new Scoreboard(TermFileInfos);
                
            scoreMap.put(word, sValue);    
        }   

        fs.close();*/
        admin.close();
        connection.close();
        return scoreMap;    
    }

    public static TermFileInfo createTermFileInfo(String strArr,double df) {
        String[] strPart = strArr.split("<div>");
        

        String fileName     = strPart[0];
        //tf*Math.log10(N/df);
        //Growel's 101<div>1.0<div>[85920942]<maindiv>Wikipedia:WikiProject Spam/LinkReports/sydroger.blogspot.com<div>2.0<div>[66489813,66490285]
        Double score        = Double.parseDouble(strPart[1])*Math.log10(N/df);
        ArrayList<Long> offset = new ArrayList<Long>();

        // Retrieve offset
        String  inputStr   = strPart[2];
        String  patternStr = "(\\d+)";
        Pattern pattern    = Pattern.compile(patternStr);
        Matcher matcher    = pattern.matcher(inputStr);
        while (matcher.find()) {
            offset.add(Long.parseLong(matcher.group().trim()));
        }
        
        return new TermFileInfo(fileName, score, offset);
    }

    public static Map<TermFileInfo,Double> createPairMap(HashMap<String, Double> weights, HashMap<String, TermFileInfo> tValueMap) {
        Map<TermFileInfo,Double> pairMap = new HashMap<TermFileInfo,Double>();
        
        
        for (String vocab : weights.keySet()) {
            pairMap.put(tValueMap.get(vocab),weights.get(vocab));
        }
        return pairMap;
    }


    /*Calculate Page number*/
    public static double rowCount()throws IOException {

        conf = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();

        double rowCount = 0.0;   
        Table table = connection.getTable(TableName.valueOf("s101062231:PageRank"));
        //HTable table = new HTable(configuration, tableName);  
        Scan scan = new Scan();  
        scan.setFilter(new FirstKeyOnlyFilter());  
        ResultScanner resultScanner = table.getScanner(scan);  
        for (Result result : resultScanner) {  
            rowCount += result.size();  
        }  

        admin.close();
        connection.close();
        return rowCount;  
    }  
    public static <K,V extends Comparable<? super V>> 
        SortedSet<Map.Entry<K,V>> entriesSortedByValues(Map<K,V> map) {
          SortedSet<Map.Entry<K,V>> sortedEntries = new TreeSet<Map.Entry<K,V>>(
              new Comparator<Map.Entry<K,V>>() {
                  @Override public int compare(Map.Entry<K,V> e1, Map.Entry<K,V> e2) {
                      int res = e2.getValue().compareTo(e1.getValue());
                      return res != 0 ? res : 1;
                  }
              }
          );
          sortedEntries.addAll(map.entrySet());
          return sortedEntries;
      }


}
